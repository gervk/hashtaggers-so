#include "UMV.h"

int32_t quit_sistema = 1;
char *mem_prin;
int32_t retardo = 0;
char alg_actual;
int32_t soc_kernel;
t_list *list_seg;
t_config *ptrConfig;
int32_t tam_mem_total;
pthread_mutex_t mutex_mem_prin = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_list_seg = PTHREAD_MUTEX_INITIALIZER;

FILE *consola_file_log;
FILE *umv_file_log;

int32_t main(){
	srand(time(0));
	//se crean los logs
	umv_file_log = txt_open_for_append("./UMV/logs/umv.log");
	txt_write_in_file(umv_file_log,"---------------------Nueva ejecucion--------------------------------------------------------------------------------------------\n");
	//crear configuracion y solicitar memoria
	ptrConfig = config_create("./UMV/umv_config.txt");
	tam_mem_total = config_get_int_value(ptrConfig,"tamanio");
	encabezado(tam_mem_total,config_get_string_value(ptrConfig,"modo"));

	txt_write_in_file(umv_file_log,"Creo la memoria principal\n");
	mem_prin = malloc(tam_mem_total);

	txt_write_in_file(umv_file_log,"Creo la lista de segmentos\n");
	list_seg = list_create();
	alg_actual = config_get_int_value(ptrConfig,"modo");

	pthread_t hilo_consola;
	pthread_t hilo_conecciones;
	//se crea el hilo para la consola
	pthread_create( &hilo_consola,  NULL,crearConsola,  NULL);
	//se crea el hilo que atendera las coneccines
	crear_hilo_detached(&hilo_conecciones, admin_conecciones,  NULL);

	//se espera a que finalize el hilo consola
	pthread_join( hilo_consola, NULL);

	txt_write_in_file(umv_file_log,"Destruyo la lista de segmentos\n");
	destruir_lista_segmento(list_seg);
	config_destroy(ptrConfig);
	txt_write_in_file(umv_file_log,"Libero la memoria principal\n");
	free(mem_prin);

	txt_write_in_file(umv_file_log,"--------------------Fin ejecucion---------------------------------------------------------------------------------------------\n");
	txt_close_file(umv_file_log);

	pthread_mutex_destroy(&mutex_mem_prin);
	pthread_mutex_destroy(&mutex_list_seg);

	return 0;
}

void crear_hilo_detached(pthread_t *hilo, void *_funcion (void *),  void *param){
	pthread_attr_t attr;

	pthread_attr_init (&attr);
	pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);
	int resp = pthread_create( hilo, &attr, _funcion, param);
	if (resp != 0)
		perror("crear hilo");
	pthread_attr_destroy (&attr);
}

void *admin_conecciones(){
	int32_t listen_soc = socket_crear_server(config_get_string_value(ptrConfig,"puerto"));
	int32_t new_soc;
	t_men_comun *men_hs;

	while(quit_sistema){
		//se recibe un mensaje y se espera
		new_soc = socket_accept(listen_soc);
		men_hs = socket_recv_comun(new_soc);
		usleep(retardo*1000);
		//se clasifica el handshake y se crea el hilo correspondiente
		switch(men_hs->tipo){
		case CONEC_CERRADA:
			printf("Socket nº%i desconectado",new_soc);
			close(new_soc);
			break;
		case HS_KERNEL:
			txt_write_in_file(umv_file_log,"SOCKETS: Kernel conectado, socket nº");
			logear_int(umv_file_log,new_soc);
			txt_write_in_file(umv_file_log,"\n");
			pthread_t hilo_conec_kernel;
			soc_kernel = new_soc;
			crear_hilo_detached(&hilo_conec_kernel, admin_conec_kernel, NULL);
			break;
		case HS_CPU:
			txt_write_in_file(umv_file_log,"SOCKETS: Nueva CPU conectada, socket nº");
			logear_int(umv_file_log,new_soc);
			txt_write_in_file(umv_file_log,"\n");
			pthread_t hilo_conec_cpu;
			t_param_conec_cpu *param_cpu = malloc(sizeof(t_param_conec_cpu));
			param_cpu->soc = new_soc;
			crear_hilo_detached(&hilo_conec_cpu, admin_conec_cpu, param_cpu);
			break;
		default:
			printf("ERROR se esperaba recibir un tipo handshake y se recibio %i\n", men_hs->tipo);
			break;
		}
		destruir_men_comun(men_hs);
	}
	socket_cerrar(listen_soc);

	return NULL;
}

void *admin_conec_kernel(){
	t_men_seg *men_seg;
	//se envia el handshake al kernel
	enviar_men_comun_destruir(soc_kernel, HS_UMV ,NULL, 0);

	while(quit_sistema){
		//se recibe un mesaje y se aguarda
		men_seg = socket_recv_seg(soc_kernel);
		usleep(retardo*1000);
		//se clasifica el tipo de operacion
		switch(men_seg->tipo){
		case CONEC_CERRADA:
			txt_write_in_file(umv_file_log,"El Kernel cerro la coneccion\n");
			quit_sistema = 0;
			break;
		case DESTR_SEGS:
			destruirSegmentos(men_seg->id_prog);
			enviar_men_comun_destruir(soc_kernel, SINCRO_OK ,NULL, 0);
			break;
		case PED_MEM_SEG_COD:
			gestionar_ped_seg(men_seg, RESP_MEM_SEG_COD);
			break;
		case PED_MEM_IND_ETI:
			gestionar_ped_seg(men_seg, RESP_MEM_IND_ETI);
			break;
		case PED_MEM_IND_COD:
			gestionar_ped_seg(men_seg, RESP_MEM_IND_COD);
			break;
		case PED_MEM_SEG_STACK:
			gestionar_ped_seg(men_seg, RESP_MEM_SEG_STACK);
			break;
		case ESCRIBIR_SEG:
			gestionar_alm_seg(men_seg->id_prog);
			break;
		default:
			printf("ERROR al recibir del kernel el tipo de dato %i\n", men_seg->tipo);
			break;
		}
		destruir_men_seg(men_seg);
	}
	return NULL;
}

void gestionar_alm_seg(int32_t id_proc){
	//se recibe el mensaje que se va a almacenar
	t_men_comun *aux_men = socket_recv_comun(soc_kernel);
	//se almacena, utilizando un semaforo para restringir el acceso a la lista de segmentos
	pthread_mutex_lock(&mutex_list_seg);
	almacenar_segmento(aux_men, id_proc);
	pthread_mutex_unlock(&mutex_list_seg);
	//se escribe el log
	txt_write_in_file(umv_file_log,"Almaceno el segmento del proceso nº");
	logear_int(umv_file_log,id_proc);
	txt_write_in_file(umv_file_log," del tipo: ");
	traducir_tipo_de_seg_y_logear(aux_men->tipo);
	txt_write_in_file(umv_file_log,"\n");

	destruir_men_comun(aux_men);

	enviar_men_comun_destruir(soc_kernel, MEN_ALM_OK ,NULL, 0);
}

void almacenar_segmento(t_men_comun *aux_men, int32_t id_proc){
	t_seg *aux_seg = buscar_segmento_tipo_seg(id_proc, aux_men->tipo);

	if (aux_seg == NULL)
		return;

	if (aux_men->tam_dato != aux_seg->tam_seg){
		printf("ERROR el tamanio:%i del segmento que se va a almacenar es distinto al tamanio:%i del segmento reservado\n",aux_men->tam_dato , aux_seg->tam_seg);
	}else{
		pthread_mutex_lock(&mutex_mem_prin);
		memcpy(&mem_prin[aux_seg->dir_fisica],aux_men->dato,aux_men->tam_dato);
		pthread_mutex_unlock(&mutex_mem_prin);
	}
}

t_seg *buscar_segmento_tipo_seg(int32_t id_proc ,int32_t tipo_seg){
	bool _es_el_proc_y_el_tipo_seg(t_seg *seg){
		return (seg->tipo_seg == tipo_seg)&&(seg->id_proc == id_proc);
	}
	//se busca el segmento que conincida en id y tipo
	t_seg *ret = list_find(list_seg, (void*)_es_el_proc_y_el_tipo_seg);

	txt_write_in_file(umv_file_log,"	Busco en la lista de segmentos, el segmento del proceso nº");
	logear_int(umv_file_log,id_proc);
	txt_write_in_file(umv_file_log," del tipo: ");
	logear_int(umv_file_log,tipo_seg);
	txt_write_in_file(umv_file_log,"\n");

	if(ret == NULL){
		printf("ERROR no se a encontrado el tipo de segmento nº%i del proceso %i\n",tipo_seg,id_proc);
		txt_write_in_file(umv_file_log,"	No se a encontrado el segmento del proceso nº");
		logear_int(umv_file_log,id_proc);
		txt_write_in_file(umv_file_log," del tipo:");
		logear_int(umv_file_log,tipo_seg);
		txt_write_in_file(umv_file_log,"\n");
	}

	return ret;
}

t_seg *buscar_segmento_dir_logica(int32_t id_proc, int32_t dir_logica){
	bool _es_el_proc_y_el_tipo_seg(t_seg *seg){
		return (seg->dir_logica == dir_logica)&&(seg->id_proc == id_proc);
	}
	//se busca el segmento que coincida con id y direccion logica
	t_seg *ret = list_find(list_seg, (void*)_es_el_proc_y_el_tipo_seg);

	txt_write_in_file(umv_file_log,"	Busco en la lista de segmentos, el segmento del proceso nº");
	logear_int(umv_file_log,id_proc);
	txt_write_in_file(umv_file_log,", con direccion logica: ");
	logear_int(umv_file_log,dir_logica);
	txt_write_in_file(umv_file_log,"\n");

	return ret;
}

void destruir_lista_segmento(t_list *lista_seg){
	void _destruir_t_seg(t_seg *seg){
		free(seg);
	}
	list_destroy_and_destroy_elements(lista_seg, (void*)_destruir_t_seg);
}

void gestionar_ped_seg(t_men_seg *men_seg,int32_t tipo_resp){
	//se crea un segmento
	int resp_dir_mem = crearSegmento(men_seg);
	char *aux_string;
	/*si no hay lugar para alguno de los segmentos, se eliminan
	todos los segmentos de ese programa - si hay lugar le envia al
	kernel la direccion logica asignada a ese segmento*/
	if (resp_dir_mem == -1){
		destruirSegmentos(men_seg->id_prog);
		enviar_men_comun_destruir(soc_kernel, MEM_OVERLOAD ,NULL, 0);
	}else{
		aux_string = string_itoa(resp_dir_mem);
		enviar_men_comun_destruir(soc_kernel, tipo_resp ,aux_string, string_length(aux_string));
		free(aux_string);
	}
}

void *admin_conec_cpu(t_param_conec_cpu *param){
	t_men_cpu_umv *men_bytes;
	int32_t proc_activo = 0, este_conectada = 1;
	//se envia al CPU el handshake inicial
	enviar_men_comun_destruir(param->soc, HS_UMV ,NULL, 0);

	while(este_conectada){
		//se recibe el mensaje y se espera
		men_bytes = socket_recv_cpu_umv(param->soc);
		usleep(retardo*1000);
		//se clasifica el mensaje que se recibe
		switch(men_bytes->tipo){
		case CONEC_CERRADA:
			este_conectada = 0;
			break;
		case CAMBIO_PA:
			txt_write_in_file(umv_file_log,"Cambio el proceso activo del proceso nº");
			logear_int(umv_file_log, proc_activo);
			proc_activo = men_bytes->base;
			txt_write_in_file(umv_file_log,", al proceso nº");
			logear_int(umv_file_log, proc_activo);
			txt_write_in_file(umv_file_log,", de la CPU nº");
			logear_int(umv_file_log, param->soc);
			txt_write_in_file(umv_file_log,"\n");
			break;
		case SOL_BYTES:
			gestionar_solicitud_bytes(param->soc, men_bytes, proc_activo);
			break;
		case ALM_BYTES:
			gestionar_almacenamiento_bytes(param->soc, men_bytes, proc_activo);
			break;
		default:
			printf("ERROR al recibir de la CPU el tipo de dato %i\n", men_bytes->tipo);
			break;
		}
		destruir_men_cpu_umv(men_bytes);
	}
	txt_write_in_file(umv_file_log,"El CPU nº");
	logear_int(umv_file_log,param->soc);
	txt_write_in_file(umv_file_log," se ha desconectado\n");
	socket_cerrar(param->soc);
	free(param);

	return NULL;
}

void gestionar_solicitud_bytes(int32_t soc_cpu,t_men_cpu_umv *men_bytes, int32_t proc_activo){

	txt_write_in_file(umv_file_log,"Solicitud de bytes por el CPU nº");
	logear_int(umv_file_log,soc_cpu);
	txt_write_in_file(umv_file_log," del proceso nº");
	logear_int(umv_file_log,proc_activo);
	txt_write_in_file(umv_file_log,", con base:");
	logear_int(umv_file_log,men_bytes->base);
	txt_write_in_file(umv_file_log,", Offset:");
	logear_int(umv_file_log,men_bytes->offset);
	txt_write_in_file(umv_file_log,", Tamanio:");
	logear_int(umv_file_log,men_bytes->tam);
	txt_write_in_file(umv_file_log,", Tipo:");
	traducir_tipo_men_bytes_y_logear(men_bytes->tipo);
	txt_write_in_file(umv_file_log,"\n");

	//se leen de memoria los bytes
	char *bytes = solicitar_bytes(proc_activo, men_bytes->base, men_bytes->offset, men_bytes->tam);

	//se envia a la CPU un mensaje con los bytes o con segmentatio fault si se inteto acceder a una posicion fuera del rango
	if (bytes == NULL){
		enviar_men_comun_destruir(soc_cpu, SEGMEN_FAULT ,NULL, 0);
	}else
		enviar_men_comun_destruir(soc_cpu, R_SOL_BYTES ,bytes, men_bytes->tam);

	free(bytes);
}

char *solicitar_bytes(int32_t id_proc, int32_t base, int32_t offset, int32_t tam){
	int32_t i;

	if ((offset < 0) || (base < 0) || (offset > tam_mem_total) || (tam > tam_mem_total))
		return NULL;

	char *ret = malloc(tam);
	pthread_mutex_lock(&mutex_list_seg);
	t_seg *aux_seg = buscar_segmento_dir_logica(id_proc, base);

	if(aux_seg == NULL){
		printf("ERROR no se a encontrado el segmento con direccion logica: %i del proceso %i\n", base, id_proc);
		txt_write_in_file(umv_file_log,"	No se a encontrado el segmento del proceso nº");
		logear_int(umv_file_log,id_proc);
		txt_write_in_file(umv_file_log,", con direccion logica: ");
		logear_int(umv_file_log, base);
		txt_write_in_file(umv_file_log,"\n");
		pthread_mutex_unlock(&mutex_list_seg);
		return ret;
	}

	int32_t pos = aux_seg->dir_fisica + offset;

	if (aux_seg->tam_seg<(offset+tam)){
		pthread_mutex_unlock(&mutex_list_seg);
		txt_write_in_file(umv_file_log,"	Segmentation Fault, con id_proc:");
		logear_int(umv_file_log,id_proc);
		txt_write_in_file(umv_file_log," ,base:");
		logear_int(umv_file_log,base);
		txt_write_in_file(umv_file_log,", offset:");
		logear_int(umv_file_log,offset);
		txt_write_in_file(umv_file_log,", tamanio:");
		logear_int(umv_file_log,tam);
		txt_write_in_file(umv_file_log,"\n");
		ret = NULL;
		return ret;
	}
	pthread_mutex_lock(&mutex_mem_prin);
	memcpy(ret, &mem_prin[pos],tam);
	pthread_mutex_unlock(&mutex_mem_prin);
	pthread_mutex_unlock(&mutex_list_seg);
	txt_write_in_file(umv_file_log,"	");
	for(i=0;i < tam;i++)
		logear_char(umv_file_log, ret[i]);
	txt_write_in_file(umv_file_log,"\n");
	return ret;
}

void gestionar_almacenamiento_bytes(int32_t soc_cpu, t_men_cpu_umv *men_bytes, int32_t proc_activo){
	int32_t resp, i;

	txt_write_in_file(umv_file_log,"Almacenar bytes por el CPU nº");
	logear_int(umv_file_log,soc_cpu);
	txt_write_in_file(umv_file_log," del proceso nº");
	logear_int(umv_file_log,proc_activo);
	txt_write_in_file(umv_file_log,", con base:");
	logear_int(umv_file_log,men_bytes->base);
	txt_write_in_file(umv_file_log,", Offset:");
	logear_int(umv_file_log,men_bytes->offset);
	txt_write_in_file(umv_file_log,", Tamanio:");
	logear_int(umv_file_log,men_bytes->tam);
	txt_write_in_file(umv_file_log,", Tipo:");
	traducir_tipo_men_bytes_y_logear(men_bytes->tipo);
	txt_write_in_file(umv_file_log,", buffer:");
	for(i=0;i < men_bytes->tam;i++)
		logear_char(umv_file_log,men_bytes->buffer[i]);
	txt_write_in_file(umv_file_log,"\n");

	resp = almacenar_bytes(proc_activo, men_bytes->base, men_bytes->offset, men_bytes->tam, men_bytes->buffer);

	if (resp==SEGMEN_FAULT){
		enviar_men_comun_destruir(soc_cpu, resp ,NULL, 0);
	}else
		enviar_men_comun_destruir(soc_cpu, resp ,NULL, 0);
}

int32_t almacenar_bytes(int32_t id_proc, int32_t base, int32_t offset, int32_t tam, char *buffer){

	if ((offset < 0) || (base < 0) || (offset > tam_mem_total) || (tam > tam_mem_total))
		return SEGMEN_FAULT;

	pthread_mutex_lock(&mutex_list_seg);
	//se busca el segmento donde coincida la direccion logica con la base
	t_seg *aux_seg = buscar_segmento_dir_logica(id_proc, base);

	int32_t pos = aux_seg->dir_fisica + offset;
	/*se controla que donde se intenta acceder no supere el limite del segmento
	si no hay problema se almacenan los bytes*/
	if (aux_seg->tam_seg<(offset+tam)){
		pthread_mutex_unlock(&mutex_list_seg);
		txt_write_in_file(umv_file_log,"	Segmentation Fault\n");
		return SEGMEN_FAULT;
	}else{
		pthread_mutex_lock(&mutex_mem_prin);
		memcpy(&mem_prin[pos],buffer,tam);
		pthread_mutex_unlock(&mutex_mem_prin);
		pthread_mutex_unlock(&mutex_list_seg);
		txt_write_in_file(umv_file_log,"	Bytes almacenados\n");
		return R_ALM_BYTES;
	}
}

void ordenar_lista_seg_por_dir_fisica(){
	bool _menor_dir_fisica(t_seg *seg1, t_seg *seg2){
		return seg1->dir_fisica < seg2->dir_fisica;
	}
	list_sort(list_seg, (void*)_menor_dir_fisica);
}

int32_t crearSegmento(t_men_seg *men_ped){
	int resp;
	pthread_mutex_lock(&mutex_list_seg);
	//se busca espacio en memoria si no hay, se compacta, y si sigue sin haber se devuelve que no hay lugar

	resp = buscar_espacio_mem_prin(men_ped->tam_seg);
	if (resp==-1){
		compactar();
		resp = buscar_espacio_mem_prin(men_ped->tam_seg);
		if (resp==-1){
			pthread_mutex_unlock(&mutex_list_seg);
			txt_write_in_file(umv_file_log,"	Memory Overload\n");
			return -1;
		}
	}
	//se crea un nuevo segmento y se agrega un nodo a la lista de segmentos
	t_seg *aux_seg = malloc(sizeof(t_seg));
	switch(men_ped->tipo){
	case PED_MEM_SEG_COD:
		aux_seg->tipo_seg = CODIGO_SCRIPT;
		break;
	case PED_MEM_IND_ETI:
		aux_seg->tipo_seg = IND_ETI_FUNC;
		break;
	case PED_MEM_IND_COD:
		aux_seg->tipo_seg = IND_COD;
		break;
	case PED_MEM_SEG_STACK:
		aux_seg->tipo_seg = IND_STACK;
		break;
	default:
		aux_seg->tipo_seg = men_ped->tipo;
		break;
	}
	aux_seg->id_proc = men_ped->id_prog;
	aux_seg->tam_seg = men_ped->tam_seg;
	aux_seg->dir_fisica = resp;
	aux_seg->dir_logica = asignarMemoriaAleatoria(men_ped->tam_seg, men_ped->id_prog);
	list_add(list_seg, aux_seg);
	pthread_mutex_unlock(&mutex_list_seg);

	txt_write_in_file(umv_file_log,"Creo el segmento del proceso nº");
	logear_int(umv_file_log,men_ped->id_prog);
	txt_write_in_file(umv_file_log,", tamanio: ");
	logear_int(umv_file_log,men_ped->tam_seg);
	txt_write_in_file(umv_file_log,", dir logica: ");
	logear_int(umv_file_log, aux_seg->dir_logica);
	txt_write_in_file(umv_file_log,", tipo: ");
	traducir_tipo_de_seg_y_logear(men_ped->tipo);
	txt_write_in_file(umv_file_log,"\n");

	return aux_seg->dir_logica;
}

//genera memmoria aleatoria y controla que no se pise la memoria
int32_t asignarMemoriaAleatoria(int32_t tamanio, int32_t id){

	int32_t ret = (rand());
	t_seg *nodo;
	int32_t x;
	int32_t memEstaOk;
	
	
	
	// se recorre la lista y se controla que los segmentos que pertenecen a ese programa no se pise, si se pisa se
	// genera un nuevo numero aleatorio y se recorre nuevamente la lista controlando hasta que todo este ok
	do{

	memEstaOk = 1;
		for (x =0;x<list_size(list_seg);x++){
			nodo = list_get(list_seg,x);
			if (nodo->id_proc == id){
				if ((((nodo->dir_logica) <= ret) && ((nodo->dir_logica + nodo->tam_seg) >= ret)) || (((nodo->dir_logica) <= ret+tamanio)
						&& ((nodo->dir_logica + nodo->tam_seg) >= ret+tamanio)) || ((ret <= nodo->dir_logica) && (ret >= (nodo->dir_logica+nodo->tam_seg)))){
						memEstaOk=0;
						ret = (rand());
				}
			}
		}
		
	}while(!memEstaOk);

	return ret;
}

int32_t buscar_espacio_mem_prin(int32_t tam_a_reservar){
	int32_t ind_mem=0,j;
	int32_t tam_seg = 0;
	t_seg *aux_seg;

	ordenar_lista_seg_por_dir_fisica();

	if (alg_actual == FIRST_FIT){
		for (j=0; j < list_size(list_seg);j++){
			aux_seg = list_get(list_seg, j);
			for(tam_seg=0; ind_mem<aux_seg->dir_fisica;ind_mem++,tam_seg++)
				if (tam_seg == tam_a_reservar)
					return ind_mem-tam_seg;
			ind_mem = ind_mem + aux_seg->tam_seg;
		}
		tam_seg = tam_mem_total-ind_mem;
		if (tam_a_reservar <= tam_seg)
			return ind_mem;
		return -1;
	}else{
		int32_t pos_seg_max = -1;
		int32_t tam_seg_max = -1;
		for (j=0; j < list_size(list_seg);j++){
			aux_seg = list_get(list_seg, j);
			for(tam_seg=0; ind_mem<aux_seg->dir_fisica;ind_mem++,tam_seg++)
				if (tam_seg > tam_seg_max){
					tam_seg_max = tam_seg;
					pos_seg_max = ind_mem-tam_seg;
				}
			ind_mem = ind_mem + aux_seg->tam_seg;
		}
		tam_seg = tam_mem_total-ind_mem;
		if (tam_seg > tam_seg_max){
			tam_seg_max = tam_seg;
			pos_seg_max = ind_mem;
		}
		if (tam_a_reservar <= tam_seg_max){
			return pos_seg_max;
		}else{
			return -1;
		}
	}
}

void compactar(){
	int i;
	int ind_mem=0, aux_dir_fisica;
	int tam_list = list_size(list_seg);
	t_seg *aux_seg;

	ordenar_lista_seg_por_dir_fisica();
	for(i=0;i < tam_list;i++){
		aux_seg = list_get(list_seg, i);
		aux_dir_fisica = ind_mem;
		if (ind_mem == aux_seg->dir_fisica){
			ind_mem= ind_mem + aux_seg->tam_seg;
		}else{
			ind_mem = aux_seg->dir_fisica;
			memcpy(mem_prin+aux_dir_fisica,&aux_seg,aux_seg->tam_seg);
			ind_mem = aux_dir_fisica+aux_seg->tam_seg;
			aux_seg->dir_fisica = aux_dir_fisica;
			list_replace(list_seg, i, aux_seg);
		}
	}
	txt_write_in_file(umv_file_log,"	Memoria compactada\n");
}

void destruirSegmentos(int id_prog){
	bool es_id_prog(t_seg *seg){
		return seg->id_proc == id_prog;
	}
	void destruir_t_seg(t_seg *seg){
		free(seg);
	}
	if (id_prog == 0){//si es 0 destruye todos los segmentos
		pthread_mutex_lock(&mutex_list_seg);
		list_clean_and_destroy_elements(list_seg, (void*)destruir_t_seg);
		pthread_mutex_unlock(&mutex_list_seg);
		txt_write_in_file(umv_file_log,"	Todos los segmentos destruido\n");
	}else{
		pthread_mutex_lock(&mutex_list_seg);
		while(list_any_satisfy(list_seg, (void*)es_id_prog))
			list_remove_and_destroy_by_condition(list_seg, (void*)es_id_prog, (void*)destruir_t_seg);
		pthread_mutex_unlock(&mutex_list_seg);
		txt_write_in_file(umv_file_log,"	Segmentos del proceso nº");
		logear_int(umv_file_log,id_prog);
		txt_write_in_file(umv_file_log," destruidos\n");
	}
}

//consola
void* crearConsola(){
	char opcion;
	consola_file_log = txt_open_for_append("./UMV/logs/consola.log");
	txt_write_in_file(consola_file_log,"--------------------Nueva ejecucion-------------------------------------------------------------------------------------------\n");
	do {
		menuPrincipal();
		do {
			scanf("%c", &opcion);
		} while (opcion<'1' || opcion>'6');

		switch (opcion) {
			case '1':
				operacion();
				break;
			case '2':
				cambiarRetardo();
				break;
			case '3':
				cambiarAlgoritmo();
				break;
			case '4':
				pthread_mutex_lock(&mutex_list_seg);
				compactar();
				pthread_mutex_unlock(&mutex_list_seg);
				break;
			case '5':
				imprimirDump();
				break;
			}
	}while (opcion != '6');
	txt_write_in_file(consola_file_log,"--------------------Fin ejecucion---------------------------------------------------------------------------------------------\n");
	txt_close_file(consola_file_log);
	//salida de la UMV
	quit_sistema = 0;

	return NULL;
}

void menuPrincipal(){
	printf ("--------------------------------\n"
			"Elija una opcion:\n"
			"	1.Operacion\n"
			"	2.Retardo\n"
			"	3.Cambiar Algoritmo\n"
			"	4.Compactacion\n"
			"	5.Dump\n"
			"	6.Salir\n");
}

void operacion(){
	char opcion;
	printf ("--------------------------------\n"
			"Que operacion desea realizar:\n"
			"	1.Crear Segmento\n"
			"	2.Destruir Segmento\n"
			"	3.Solicitar Memoria\n"
			"	4.Escribir Memoria\n");
	do {
		scanf("%c", &opcion);
	} while (opcion<'1' || opcion>'4');

	switch (opcion) {
	case '1':
	case '2':
		operacion_segmentos(opcion);
		break;
	case '3':
	case '4':
		operacion_memoria(opcion);
		break;
	}
}

void operacion_segmentos(char opcion){
	int32_t tipo_seg, tam, id_proc, dir_logica;
	t_men_seg *men_seg;

	if (opcion == '1'){
		printf("ID del proceso:");
		scanf("%i",&id_proc);
		printf("Tipo de segmento:");
		scanf("%i",&tipo_seg);
		printf("Tamaño:");
		scanf("%i",&tam);
		txt_write_in_file(consola_file_log,"Creo segmento con ID proceso: ");
		logear_int(consola_file_log,id_proc);
		txt_write_in_file(consola_file_log,", tipo: ");
		logear_int(consola_file_log,tipo_seg);
		txt_write_in_file(consola_file_log,", tamanio: ");
		logear_int(consola_file_log,tam);
		txt_write_in_file(consola_file_log,"\n");
		men_seg = crear_men_seg(tipo_seg, id_proc, tam);
		dir_logica = crearSegmento(men_seg);
		if(dir_logica == -1){
			txt_write_in_file(consola_file_log,"	Memory overload\n");
		}else{
			txt_write_in_file(consola_file_log,"	Direccion logica del segmento: ");
			logear_int(consola_file_log,dir_logica);
			txt_write_in_file(consola_file_log,"\n");
		}
		destruir_men_seg(men_seg);
	}else{
		printf ("--------------------------------\n"
				"Ingrese 0 si desea destruir todos\n"
				"ID proceso: ");
		scanf("%i",&id_proc);
		if (id_proc == 0){
			txt_write_in_file(consola_file_log,"Destruyo todos los segmentos de la tabla\n");
		}else{
			txt_write_in_file(consola_file_log,"Destruyo los segmentos del proceso nº");
			logear_int(consola_file_log,id_proc);
			txt_write_in_file(consola_file_log,"\n");
		}
		destruirSegmentos(id_proc);
	}
}

void operacion_memoria(char opcion){
	int32_t tipo_seg = 0, offset = 0, tam = 0, id_proc = 0,i = 0, resp = 0, base = 0;
	char *buffer;
	t_seg *aux_seg;

	printf("ID del proceso:");
	scanf("%i",&id_proc);
	printf("Tipo de segmento:");
	scanf("%i",&tipo_seg);
	printf("Offset:");
	scanf("%i",&offset);
	printf("Tamanio:");
	scanf("%i",&tam);

	if (opcion == '3'){
		txt_write_in_file(consola_file_log,"Imprimo la memoria del proceso nº: ");
		logear_int(consola_file_log,id_proc);
		txt_write_in_file(consola_file_log,", Tipo de segmento: ");
		logear_int(consola_file_log,tipo_seg);
		txt_write_in_file(consola_file_log,", Offset: ");
		logear_int(consola_file_log,offset);
		txt_write_in_file(consola_file_log,", Tamanio: ");
		logear_int(consola_file_log,tam);
		txt_write_in_file(consola_file_log,"\n");

		aux_seg = buscar_segmento_tipo_seg(id_proc ,tipo_seg);
		if (aux_seg == NULL){
			txt_write_in_file(consola_file_log,"	ERROR no se a encontrado el tipo de segmento nº");
			logear_int(consola_file_log,tipo_seg);
			txt_write_in_file(consola_file_log," del proceso ");
			logear_int(consola_file_log,id_proc);
			txt_write_in_file(consola_file_log,"\n");
			return;
		}
		base = aux_seg->dir_logica;
		buffer = solicitar_bytes(id_proc, base, offset,  tam);
		if (buffer == NULL){
			txt_write_in_file(consola_file_log,"	SEGMENTATION FAULT\n");
		}else{
			txt_write_in_file(consola_file_log,"	");
			for (i=0;i<tam;i++)
				logear_char(consola_file_log,buffer[i]);
			txt_write_in_file(consola_file_log,"\n");
		}
		free(buffer);
	}else{
		txt_write_in_file(consola_file_log,"Escribo la memoria del proceso nº: ");
		logear_int(consola_file_log,id_proc);
		txt_write_in_file(consola_file_log,", Tipo de segmento: ");
		logear_int(consola_file_log,tipo_seg);
		txt_write_in_file(consola_file_log,", Offset: ");
		logear_int(consola_file_log,offset);
		txt_write_in_file(consola_file_log,", Tamanio: ");
		logear_int(consola_file_log,tam);
		txt_write_in_file(consola_file_log,", Buffer: ");
		printf("Buffer:");
		while ((i = getchar()) != '\n' && i != EOF);//esta linea es para limpiar el buffer, por que puede llegar a quedar un \n
		buffer = malloc(tam+1);
		txt_write_in_file(consola_file_log,fgets(buffer, tam+1, stdin));
		txt_write_in_file(consola_file_log,"\n");

		aux_seg = buscar_segmento_tipo_seg(id_proc ,tipo_seg);
		if (aux_seg == NULL){
			txt_write_in_file(consola_file_log,"	ERROR no se a encontrado el tipo de segmento nº");
			logear_int(consola_file_log,tipo_seg);
			txt_write_in_file(consola_file_log," del proceso ");
			logear_int(consola_file_log,id_proc);
			txt_write_in_file(consola_file_log,"\n");
			free(buffer);
			return;
		}
		base = aux_seg->dir_logica;
		resp = almacenar_bytes(id_proc, base, offset, tam, buffer);
		if (resp == SEGMEN_FAULT){
			txt_write_in_file(consola_file_log,"	Segmentation Fault\n");
		}else
			txt_write_in_file(consola_file_log,"	Se escribio corectamente\n");
		free(buffer);
	}
}

void cambiarAlgoritmo(){
	if(alg_actual == WORST_FIT){
		alg_actual = FIRST_FIT;
		printf("Algoritmo cambiado a = First-Fit\n");
	}else{
		alg_actual = WORST_FIT;
		printf("Algoritmo cambiado a = Worst-Fit\n");
	}
}

void cambiarRetardo(){
	int32_t t;

	printf ("--------------------------------\n");
	printf("Ingrese el tiempo:");
	if (scanf("%i",&t)!= EOF && t>=0){
		retardo = t;
		printf("Nuevo retardo: %i\n", t);
	}else{
		printf("Tiempo no válido.\n");
	}
}

void imprimirDump(){
	char opcion;
	printf ("--------------------------------\n"
			"Elija lo que desea imprimir:\n"
			"	1.Estructuras de memoria\n"
			"	2.Memoria principal\n"
			"	3.Contenido memoria principal\n");
	do {
		scanf("%c", &opcion);
	} while (opcion<'1' || opcion>'3');

	switch (opcion) {
	case '1':
		imp_estructura_mem();
		break;
	case '2':
		imp_mem_prin();
		break;
	case '3':
		imp_cont_mem_prin();
		break;
	}
}

void imp_estructura_mem(){
	int id_prog;

	printf ("--------------------------------\n"
			"Ingrese 0 si desea imprimir todos\n"
			"ID proceso: ");
	scanf("%i",&id_prog);
	if (id_prog != 0){
		txt_write_in_file(consola_file_log,"Imprimo los segmentos del proceso nº");
		logear_int(consola_file_log,id_prog);
		txt_write_in_file(consola_file_log,"\n");
	}else{
		txt_write_in_file(consola_file_log,"Imprimo todos los segmentos de los procesos\n");
	}
	imp_tablas_segmentos(id_prog);
}

void imp_tablas_segmentos(int32_t id_prog){
	bool _es_del_proc(t_seg *seg){
		return seg->id_proc == id_prog;
	}
	void imp_seg(t_seg *seg){
		txt_write_in_file(consola_file_log,"	");
		logear_int(consola_file_log,seg->tipo_seg);
		txt_write_in_file(consola_file_log,"	|");
		logear_int(consola_file_log, seg->id_proc);
		txt_write_in_file(consola_file_log,"	|");
		logear_int(consola_file_log, seg->dir_fisica);
		txt_write_in_file(consola_file_log,"	|");
		logear_int(consola_file_log, seg->tam_seg);
		txt_write_in_file(consola_file_log,"	|");
		logear_int(consola_file_log,seg->dir_logica);
		txt_write_in_file(consola_file_log,"\n");
	}
	pthread_mutex_lock(&mutex_list_seg);

	if (id_prog == 0){//imprimo todos los segementos de los programas
		if(list_is_empty(list_seg))
			txt_write_in_file(consola_file_log,"	Lista de segmentos vacia\n");
		else{
			txt_write_in_file(consola_file_log,"	TIPO	|ID	|DIR_F	|TAM	|DIR_L\n");
			list_iterate(list_seg, (void *)imp_seg);
		}
	}else{//imprimo los segementos de un programa especifico
		if (list_find(list_seg, (void*)_es_del_proc) == NULL)//si el programa no existe
			txt_write_in_file(consola_file_log,"	El proceso no existe\n");
		else{//si existe el programa imprimo sus segmentos
			txt_write_in_file(consola_file_log,"	TIPO	|ID	|DIR_F	|TAM	|DIR_L\n");
			txt_write_in_file(consola_file_log,"	");
			list_iterate(list_filter(list_seg, (void*)_es_del_proc), (void *)imp_seg);
		}
	}
	pthread_mutex_unlock(&mutex_list_seg);
}

void imp_mem_prin(){
	int32_t ind_mem=0,j,i,anterior = 0;
	t_seg *aux_seg;
	pthread_mutex_lock(&mutex_list_seg);
	ordenar_lista_seg_por_dir_fisica();

	txt_write_in_file(consola_file_log,"Imprimo la memoria principal\n");
	txt_write_in_file(consola_file_log,"	");
	for (j=0; j < list_size(list_seg);j++){
		aux_seg = list_get(list_seg, j);
		for(;ind_mem < aux_seg->dir_fisica;ind_mem++){
			if (anterior){
				txt_write_in_file(consola_file_log,"▄");
				anterior = 0;
			}else{
				txt_write_in_file(consola_file_log,"▀");
				anterior = 1;
			}
		}
		logear_int(consola_file_log, aux_seg->id_proc);
		for(i=0;i < aux_seg->tam_seg;i++,ind_mem++)
			txt_write_in_file(consola_file_log,"█");
	}
	for(;ind_mem < tam_mem_total;ind_mem++)
		if (anterior){
			txt_write_in_file(consola_file_log,"▄");
			anterior = 0;
		}else{
			txt_write_in_file(consola_file_log,"▀");
			anterior = 1;
		}
	txt_write_in_file(consola_file_log,"\n");
	pthread_mutex_unlock(&mutex_list_seg);
}

void imp_cont_mem_prin(){
	int32_t offset, tam, i;

	printf ("Offset: ");
	scanf("%i",&offset);
	printf ("Cantidad de bytes: ");
	scanf("%i",&tam);
	txt_write_in_file(consola_file_log,"Imprimo contenido de la memoria principal, Offset:");
	logear_int(consola_file_log,offset);
	txt_write_in_file(consola_file_log,", Cantidad de bytes:");
	logear_int(consola_file_log,tam);
	txt_write_in_file(consola_file_log,"\n");
	if (tam + offset > tam_mem_total){
		txt_write_in_file(consola_file_log,"	Segmentation Fault\n");
	}else{
		pthread_mutex_lock(&mutex_mem_prin);
		txt_write_in_file(consola_file_log,"	");
		for(i=offset;i<(offset+tam);i++)
			logear_char(consola_file_log,mem_prin[i]);
		txt_write_in_file(consola_file_log,"\n");
		pthread_mutex_unlock(&mutex_mem_prin);
	}
}

void encabezado(long byte, char *modo){
	printf("---------------** UMV: Unidad de Memoria Virtual **---------------\n");
	printf("Puerto: %s\n", config_get_string_value(ptrConfig,"puerto"));
	printf("Operando en modo: %s\n", modo);
	printf("Espacio reservado: %ld bytes\n", byte);
	printf("------------------------------------------------------------------\n\n");
}

void traducir_tipo_de_seg_y_logear(int32_t tipo){
	switch(tipo){
	case PED_MEM_SEG_COD:
	case CODIGO_SCRIPT:
		txt_write_in_file(umv_file_log,"CODIGO_SCRIPT");
		break;
	case PED_MEM_IND_ETI:
	case IND_ETI_FUNC:
		txt_write_in_file(umv_file_log,"IND_ETI_FUNC");
		break;
	case PED_MEM_IND_COD:
	case IND_COD:
		txt_write_in_file(umv_file_log,"IND_COD");
		break;
	case PED_MEM_SEG_STACK:
		txt_write_in_file(umv_file_log,"IND_STACK");
		break;
	default:
		logear_int(umv_file_log,tipo);
		txt_write_in_file(umv_file_log," (PUEDE QUE HAYA UN ERROR)");
		break;
	}
}

void traducir_tipo_men_bytes_y_logear(int32_t tipo){
	switch(tipo){
	case SOL_BYTES:
		txt_write_in_file(umv_file_log,"SOL_BYTES");
		break;
	case ALM_BYTES:
		txt_write_in_file(umv_file_log,"ALM_BYTES");
		break;
	default:
		logear_int(umv_file_log,tipo);
		txt_write_in_file(umv_file_log," (PUEDE QUE HAYA UN ERROR)");
		break;
	}
}

void logear_int(FILE* destino,int32_t un_int){
	char *aux_string = string_itoa(un_int);
	txt_write_in_file(destino,aux_string);
	free(aux_string);
}

void logear_char(FILE* destino,char un_char){
	if (un_char == '\0'){
		char *aux_string = "\\0";
		txt_write_in_file(destino, aux_string);
	}else{
		char *aux_string = string_itoa((int)un_char);
		txt_write_in_file(destino, aux_string);
		free(aux_string);
	}
	txt_write_in_file(destino,"-");
}
