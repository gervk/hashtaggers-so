#include "cpu.h"
AnSISOP_funciones functions = {
		.AnSISOP_definirVariable		= definirVariable,
		.AnSISOP_obtenerPosicionVariable= obtenerPosicionVariable,
		.AnSISOP_dereferenciar			= dereferenciar,
		.AnSISOP_asignar				= asignar,
		.AnSISOP_obtenerValorCompartida = obtenerValorCompartida,
		.AnSISOP_asignarValorCompartida = asignarValorCompartida,
		.AnSISOP_irAlLabel              = irAlLabel,
		.AnSISOP_llamarSinRetorno       =llamarSinRetorno,
		.AnSISOP_llamarConRetorno       = llamarConRetorno,
		.AnSISOP_finalizar              = finalizar,
		.AnSISOP_retornar               = retornar,
		.AnSISOP_imprimir				= imprimir,
		.AnSISOP_imprimirTexto			= imprimirTexto,
		.AnSISOP_entradaSalida 			= entradaSalida};

AnSISOP_kernel kernel_functions = {
		.AnSISOP_wait 	= wait,
		.AnSISOP_signal = mi_signal};

enum {	BARRA_CERO = 1 };
enum tipo_fin {
	ERROR = -1,
	OK = 0
};

t_dictionary *dic_Variables;
t_pcb *pcb = NULL;
int32_t socketKernel, socketUmv;
int32_t sem_block = 0;//todo no implementado
int32_t quantum_actual;
int32_t quantum_max;
char* etiquetas;
int32_t fueFinEjecucion = 0, entre_io = 0, huboSegFault = 0;
FILE *cpu_file_log;
int32_t pid_cpu;
int32_t quit_sistema = 1;

int main(){
	char *proxInstrucc;

	dic_Variables = dictionary_create();

	cpu_file_log = txt_open_for_append("./CPU/logs/cpu.log");
	txt_write_in_file(cpu_file_log,"---------------------Nueva ejecucion------------------------------\n");

	txt_write_in_file(cpu_file_log, "Cargo la configuracion desde el archivo\n");

	t_config *unaConfig = config_create("./CPU/cpu_config");
	char *puertoKernel = config_get_string_value(unaConfig, "Puerto_Kernel");
	char *ipKernel = config_get_string_value(unaConfig, "IP_Kernel");
	char *puertoUmv = config_get_string_value(unaConfig, "Puerto_UMV");
	char *ipUmv = config_get_string_value(unaConfig, "IP_UMV");
	imprimo_config(puertoKernel, ipKernel, puertoUmv, ipUmv);

	printf("El PID de este CPU es %d \n", getpid());

	/*conexion con el kernel*/
	handshake_kernel(puertoKernel, ipKernel);

	/*conexion con la umv*/
	handshake_umv(puertoUmv, ipUmv);

	/*maneja si recibe la señal SIGUSR, hay que revisarlo todavia*/
	signal(SIGUSR1, signal_handler);
	signal(SIGINT, signal_handler);

	while(quit_sistema){
		/*recibe un pcb del kernel*/
		recibirUnPcb();
		if(pcb == NULL)
			goto _fin;
		/* le digo a la UMV q cambie el proceso activo */
		cambio_PA(pcb->id);

		traerIndiceEtiquetas();
		/*se crea un diccionario para guardar las variables del contexto*/
		regenerar_dicc_var();

		for (quantum_actual = 0;(quantum_actual < quantum_max) && (!fueFinEjecucion) && (!entre_io) && (!sem_block); quantum_actual++){//aca cicla hasta q el haya terminado los quantums
			printf("Quantum nº%i\n",quantum_actual+1);
			proxInstrucc = solicitarProxSentenciaAUmv();
			analizadorLinea(proxInstrucc, &functions, &kernel_functions);
			free(proxInstrucc);
		}
		if(!fueFinEjecucion){
			salirPorQuantum();
		}
		free(etiquetas);
		free(pcb);
		pcb = NULL;
		cambio_PA(0);//lo cambio a 0 asi la UMV puede comprobar q hay un error
		fueFinEjecucion = 0;
		huboSegFault = 0;
		entre_io = 0;
		sem_block = 0;
	}
	socket_cerrar(socketKernel);
	socket_cerrar(socketUmv);
	_fin:
	return 0;
}

void signal_handler(int sig){
	quit_sistema = 0;
	if (pcb == NULL){
		socket_cerrar(socketKernel);
		socket_cerrar(socketUmv);
	}
}

void traerIndiceEtiquetas(){
	int32_t base, offset, tam;
	base =pcb->dir_indice_etiquetas;
	offset=0;
	tam=pcb->tam_indice_etiquetas;

	enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, tam, NULL);

	t_men_comun *rec_etiq = socket_recv_comun(socketUmv);
	if(rec_etiq->tipo == R_SOL_BYTES){
		etiquetas = malloc(rec_etiq->tam_dato);
		memcpy(etiquetas ,rec_etiq->dato, rec_etiq->tam_dato);
	}
	if(rec_etiq->tipo == SEGMEN_FAULT){
		finalizarContexto(SEGMEN_FAULT);
	}
	destruir_men_comun(rec_etiq);
}

void recibirUnPcb(){
	int32_t length = 0;
	char *aux_len = malloc(sizeof(int32_t));
	int resultado_recv = recv(socketKernel, aux_len, sizeof(int32_t), MSG_PEEK);
	if (resultado_recv == -1) return;
	if (resultado_recv == 0) return;
	memcpy(&length, aux_len, sizeof(int32_t));
	free(aux_len);
	char stream[length];
	resultado_recv = recv(socketKernel, stream, length, MSG_WAITALL);
	if (resultado_recv == -1) return;
	if (resultado_recv == 0) return;
	if (resultado_recv != length) printf("NO PUDE RECIBIR TODO\n");

	t_men_quantum_pcb *men_pcb = men_deserealizer_quantum_pcb(stream);

	pcb = malloc(sizeof(t_pcb));
	if((men_pcb->tipo==CONEC_CERRADA) || (men_pcb->tipo!=PCB_Y_QUANTUM)){
		printf("	ERROR se ha perdido la coneccion con el Kernel o el tipo de dato recibido es erroneo\n");
		free(pcb);
	}else{
		memcpy(pcb, men_pcb->pcb, sizeof(t_pcb));
		memcpy(&quantum_max , &(men_pcb->quantum), sizeof(int32_t));
	}

	destruir_quantum_pcb(men_pcb);
}

char* solicitarProxSentenciaAUmv(){// revisar si de hecho devuelve la prox instruccion(calculos)
	int32_t base, offset, tam;
	t_men_comun *men_base_offset;

	//con el indice de codigo y el pc obtengo la posicion de la proxima instruccion a ejecutar
	base = pcb->dir_indice_codigo;
	offset = pcb->program_counter * sizeof(t_intructions);
	tam = sizeof(t_intructions);

	enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, tam, NULL);

	men_base_offset = socket_recv_comun(socketUmv);

	memcpy(&offset, men_base_offset->dato, sizeof(int32_t));
	memcpy(&tam, men_base_offset->dato+sizeof(int32_t), sizeof(int32_t));

	if((men_base_offset->tam_dato != 8) || (men_base_offset->tipo!=R_SOL_BYTES))//por si la UMV me llega a mandar un tamanio distinto al q pedi o si el tipo de dato es diferente
		printf("ERROR el tamanio recibido:%i es distinto a 8, o el tipo de dato:%i es distinto a R_SOL_BYTES\n",men_base_offset->tam_dato,men_base_offset->tipo);

	destruir_men_comun(men_base_offset);

	base = pcb->dir_seg_codigo;

	//le mando a la umv base, offset y tamanio de la proxima instruccion
	enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, tam, NULL);

	//recibir la instruccion a ejecutar
	t_men_comun *rec_inst = socket_recv_comun(socketUmv);

	char proxInst[tam+BARRA_CERO];

	if(rec_inst->tipo == R_SOL_BYTES){
		memcpy(proxInst, rec_inst->dato, tam);
		proxInst[tam] = '\0';
		char *proxInst_sin_blancos = sacar_caracteres_escape(proxInst);
		txt_write_in_file(cpu_file_log, "Instruccion que voy a ejecutar\n");
		printf("Instruccion que voy a ejecutar:%s\n",proxInst_sin_blancos);
		pcb->program_counter ++;
		destruir_men_comun(rec_inst);
		return proxInst_sin_blancos;
	}
	if(rec_inst->tipo == SEGMEN_FAULT){
		finalizarContexto(SEGMEN_FAULT);
		destruir_men_comun(rec_inst);
		return NULL;
	}
	printf("ERROR tipo de dato:%i, recibido es erroneo\n", rec_inst->tipo);
	return NULL;
}

void salirPorQuantum(){
	//mando el pcb con un tipo de mensaje que sali por quantum
	enviar_men_comun_destruir(socketKernel, FIN_QUANTUM, NULL, 0);
	enviar_pcb_destruir();
}

void enviar_pcb_destruir(){
	t_men_quantum_pcb *men = crear_men_quantum_pcb(PCB_Y_QUANTUM,0, pcb);
	socket_send_quantum_pcb(socketKernel, men);
	destruir_quantum_pcb(men);
}

void preservarContexto(){
	int32_t base, offset, tam;
	base = pcb->dir_seg_stack;
	offset = pcb->dir_cont_actual - base + (pcb->cant_var_cont_actual*5);	// la posicion siguiente al ultimo valor de la ultima varaible
	tam = sizeof(int32_t);

	char *buffer = copiar_int_to_buffer(pcb->dir_cont_actual);
	enviar_men_cpu_umv_destruir(ALM_BYTES, base, offset, tam, buffer);
	free(buffer);

	t_men_comun *r_con = socket_recv_comun(socketUmv);
	if (r_con->tipo == R_ALM_BYTES){
		destruir_men_comun(r_con);
		offset = offset + 4;
		char *buffer = copiar_int_to_buffer(pcb->program_counter);
		enviar_men_cpu_umv_destruir(ALM_BYTES, base, offset, tam, buffer);
		free(buffer);

		t_men_comun *r_proxins = socket_recv_comun(socketUmv);
		if(r_proxins->tipo == R_ALM_BYTES){
			txt_write_in_file(cpu_file_log, "Se almaceno correctamente el contexto\n");
			printf("	Se almaceno correctamente el contexto\n");
			destruir_men_comun(r_proxins);
			return;
		}
		if(r_proxins->tipo == SEGMEN_FAULT){
			finalizarContexto(SEGMEN_FAULT);
			destruir_men_comun(r_proxins);
			return;
		}
	}
	if(r_con->tipo == SEGMEN_FAULT){
		finalizarContexto(SEGMEN_FAULT);
		destruir_men_comun(r_con);
		return;
	}
	printf("	ERROR tipo de dato:%i, recibido es erroneo\n", r_con->tipo);
}

void finalizarContexto(int32_t tipo_fin){
	int32_t i, base, offset;
	char *aux_string;

	switch(tipo_fin){
	case SEM_INEX:
		printf("	ERROR,el semaforo es inexistente\n");
		fueFinEjecucion=1;
		break;
	case SEM_BLOQUEADO:
		fueFinEjecucion=1;
		enviar_pcb_destruir();
		break;
	case VAR_INEX:
		printf("	ERROR,la variable global es inexistente\n");
		fueFinEjecucion=1;
		break;
	case ERROR:
		printf("	ERROR, la etiqueta no se ha encontrado pero creo q esta fuera del alcanse del tp\n");
		fueFinEjecucion = 1;
		break;
	case SEGMEN_FAULT:
		aux_string = string_itoa(pcb->id);
		printf("Hubo un error en la solictud de memoria, se finalizará la ejecucion del programa actual %d\n", pcb->id);
		txt_write_in_file(cpu_file_log, "Hubo un error en la solictud de memoria, se finalizará la ejecucion del programa actual\n");
		enviar_men_comun_destruir(socketKernel, SEGMEN_FAULT, aux_string, string_length(aux_string)+BARRA_CERO);
		free(aux_string);
		fueFinEjecucion = 1;
		huboSegFault = 1;
		break;
	case OK:
		printf("	Imprmiendo variables y finalizando proceso\n");
		char *string = "\n----------Imprimo el estado final de las variable----------\n";

		enviar_men_comun_destruir(socketKernel, IMPRIMIR_TEXTO, string, string_length(string)+BARRA_CERO);
		char *aux_string = string_itoa(pcb->id);
		enviar_men_comun_destruir(socketKernel, ID_PROG, aux_string, string_length(aux_string)+BARRA_CERO);
		free(aux_string);
		recibir_resp_kernel(R_IMPRIMIR);

		for(i=0;i<pcb->cant_var_cont_actual;i++){//esta MIERDA de for es para imprimir vas variables en la consola del proceso progrma
			base = pcb->dir_cont_actual;
			offset = (5*i);
			enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, 1, NULL);
			t_men_comun *men_nombre_var = socket_recv_comun(socketUmv);

			base = pcb->dir_cont_actual;
			offset = (5*i)+1;

			enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, 4, NULL);
			t_men_comun *men_cont_var = socket_recv_comun(socketUmv);

			int32_t cont_var;
			memcpy(&cont_var,men_cont_var->dato,4);

			char *aux_cont_var =string_itoa(cont_var);

			int32_t tam_string = string_length(aux_cont_var)+3;// 1(nombre de la variable)+1(el igual)+string_length(aux_cont_var)+1(el \n)+1(el \0)
			char var_a_imp_con_contexto[tam_string];
			var_a_imp_con_contexto[0] = men_nombre_var->dato[0];
			var_a_imp_con_contexto[1] = '=';
			memcpy(var_a_imp_con_contexto+2,aux_cont_var,string_length(aux_cont_var));
			var_a_imp_con_contexto[tam_string-1] = '\0';
			free (aux_cont_var);

			enviar_men_comun_destruir(socketKernel, IMPRIMIR_TEXTO, var_a_imp_con_contexto, string_length(var_a_imp_con_contexto)+BARRA_CERO);
			char *aux_string = string_itoa(pcb->id);
			enviar_men_comun_destruir(socketKernel, ID_PROG, aux_string, string_length(aux_string)+BARRA_CERO);
			free(aux_string);
			recibir_resp_kernel(R_IMPRIMIR);

			destruir_men_comun(men_cont_var);
			destruir_men_comun(men_nombre_var);
		}
		aux_string = string_itoa(pcb->id);
		enviar_men_comun_destruir(socketKernel, FIN_EJECUCION, aux_string, string_length(aux_string)+BARRA_CERO);
		free(aux_string);
		fueFinEjecucion = 1;
		break;
	default:
		printf("ERROR: tipo_fin:%i inexistente\n",tipo_fin);
		break;
	}
}

char es_numero_pasar_char(char un_char){
	switch(un_char){
	case 0: case 1:	case 2:	case 3:	case 4:	case 5:	case 6:	case 7:	case 8:	case 9:
		un_char = un_char + '0';
		return un_char;
	default:
		return un_char;
	}
}

//Primitivas ANSISOP
t_puntero definirVariable(t_nombre_variable id_var){
	int32_t base, offset, tam;

	base = pcb->dir_seg_stack;
	offset= pcb->dir_cont_actual - base + (pcb->cant_var_cont_actual*5);
	tam = 5;

	int32_t pos_mem = base + offset;

	printf("	Definir la variable %c en la posicion %i, offset:%i\n",id_var, pos_mem,offset);
	id_var = es_numero_pasar_char(id_var);

	id_var = es_numero_pasar_char(id_var);
	char *key = string_substring_until(&id_var, 1);

	enviar_men_cpu_umv_destruir(ALM_BYTES, base, offset, tam, key);

	t_men_comun *r_alm = socket_recv_comun(socketUmv);

	if(r_alm->tipo == R_ALM_BYTES){
		int32_t *pos = malloc(sizeof(int32_t));
		*pos = pos_mem;
		dictionary_put(dic_Variables, key, pos);
		free(key);
		(pcb->cant_var_cont_actual) ++;
		txt_write_in_file(cpu_file_log, "Definiar la variable \n");
		txt_write_in_file(cpu_file_log, "en la posicion \n");
		destruir_men_comun(r_alm);
		return pos_mem;
	}else{
		finalizarContexto(SEGMEN_FAULT);
		destruir_men_comun(r_alm);
		free(key);
		return -1;
	}
}

t_puntero obtenerPosicionVariable(t_nombre_variable identificador_variable){
	/*Se fija en el diccionario de variables la posicion la posicion va a ser el data del elemento*/
	identificador_variable = es_numero_pasar_char(identificador_variable);
	char *key = string_substring_until(&identificador_variable, 1);

	int32_t *dir_mem = dictionary_get(dic_Variables,key);
	int32_t ret = *dir_mem;

	if (dir_mem == NULL)
		printf("	ERROR no se ha podido encontrar la key:%s\n",key);

	txt_write_in_file(cpu_file_log, "La posicion de la variable\n");
	txt_write_in_file(cpu_file_log, "es\n");
	printf("	La posicion de la variable: %s, es %i\n", key, ret);
	free(key);
	return ret;
}

t_valor_variable dereferenciar(t_puntero direccion_variable){
	//obtiene los cuatro bytes siguientes a direccion_variable
	t_valor_variable valor;

	int32_t base = pcb->dir_seg_stack;
	int32_t offset = direccion_variable-base+1;
	int32_t tam= sizeof(int32_t);

	enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, tam, NULL);

	//recibir de umv 4 bytes de valor_variable
	t_men_comun *men_comun = socket_recv_comun(socketUmv);

	if (men_comun->tipo == R_SOL_BYTES){
		memcpy(&valor, men_comun->dato,men_comun->tam_dato);
		txt_write_in_file(cpu_file_log, "Dereferenciar \n");
		txt_write_in_file(cpu_file_log, "y su valor es \n");
		printf("	Dereferenciar %d, offset:%i y su valor es: %d\n",direccion_variable, offset,valor);
	}
	if(men_comun->tipo ==SEGMEN_FAULT){
		finalizarContexto(SEGMEN_FAULT);
	}
	destruir_men_comun(men_comun);
	return valor;
}

void asignar(t_puntero direccion_variable, t_valor_variable valor){
	//envio a la umv para almacenar base= contexto actual offset= direccion_variable tamaño=4bytes buffer= valor
	if(!huboSegFault){
		int32_t base = pcb->dir_seg_stack;
		int32_t offset = direccion_variable-base+1;

		int32_t tam = sizeof(t_valor_variable);
		char *buffer = copiar_int_to_buffer(valor);
		enviar_men_cpu_umv_destruir(ALM_BYTES, base, offset, tam, buffer);
		free(buffer);

		t_men_comun *r_alm = socket_recv_comun(socketUmv);

		if(r_alm->tipo == R_ALM_BYTES){
			txt_write_in_file(cpu_file_log, "Asignando en la direccion \n");
			txt_write_in_file(cpu_file_log, "el valor \n");
			printf("	Asignando en la direccion %d, offset:%i, el valor %d \n", direccion_variable, offset, valor);
		}
		if(r_alm->tipo == SEGMEN_FAULT){
			finalizarContexto(SEGMEN_FAULT);
		}
		destruir_men_comun(r_alm);
	}
}

t_valor_variable obtenerValorCompartida(t_nombre_compartida variable){
	//le mando al kernel el nombre de la variable compartida
	enviar_men_comun_destruir(socketKernel, OBTENER_VALOR, variable, string_length(variable)+BARRA_CERO);

	//recibo el valor
	t_men_comun *respuesta = socket_recv_comun(socketKernel);
	t_valor_variable valor;

	if((respuesta->tipo != VAR_INEX) && (respuesta->tipo != R_OBTENER_VALOR))
		printf("	ERROR se ha recibido un tipo de dato erroneo\n");

	if(respuesta->tipo == VAR_INEX){
		txt_write_in_file(cpu_file_log, "Acceso a variable global inexistente \n");
		printf("	Error en acceso a la variable %s, no existe", variable);
		valor = 0;//todo asigno 0 si la variable no existe
		finalizarContexto(VAR_INEX);//todo romper
	}else{
		valor = atoi(respuesta->dato);

		txt_write_in_file(cpu_file_log, "Obteniendo el valor de compartida \n");
		txt_write_in_file(cpu_file_log, "el valor \n");
		printf("	Obteniendo el valor de compartida %s que es %i \n", variable, valor);
	}
	destruir_men_comun(respuesta);

	return valor;
}

t_valor_variable asignarValorCompartida(t_nombre_compartida variable, t_valor_variable valor){
	//le mando al kernel el nombre de la variable compartida
	//le mando el valor que le quiero asignar

	enviar_men_comun_destruir(socketKernel, GRABAR_VALOR, variable, string_length(variable)+BARRA_CERO);

	t_men_comun *men_resp = socket_recv_comun(socketKernel);
	if(men_resp->tipo == VAR_INEX){
		txt_write_in_file(cpu_file_log, "Acceso a variable global inexistente \n");
		printf("	Error en acceso a la variable %s, no existe", variable);
		finalizarContexto(VAR_INEX);
		destruir_men_comun(men_resp);
		return -1;
	}
	if(men_resp->tipo == R_GRABAR_VALOR){
		destruir_men_comun(men_resp);
		char *c =  string_itoa(valor);
		enviar_men_comun_destruir(socketKernel, VALOR_ASIGNADO, c, string_length(c)+BARRA_CERO);
		free(c);
		men_resp = socket_recv_comun(socketKernel);
		destruir_men_comun(men_resp);
		return valor;
	}
	printf("	ERROR el kernel me mando:%i y se esperaba otro tipo\n",men_resp->tipo);
	return -1;
}

void irAlLabel(t_nombre_etiqueta nombre_etiqueta){//revisar
	// primer instruccion ejecutable de etiqueta y -1 en caso de error
	char *etiq_sin_blancos = sacar_caracteres_escape(nombre_etiqueta);

	//t_puntero_instruccion pos_etiqueta= metadata_buscar_etiqueta("inicio", etiquetas, pcb->tam_indice_etiquetas);
	t_puntero_instruccion pos_etiqueta= metadata_buscar_etiqueta(etiq_sin_blancos, etiquetas, pcb->tam_indice_etiquetas);

	if(pos_etiqueta != -1){
		pcb->program_counter = pos_etiqueta;
		txt_write_in_file(cpu_file_log, "Yendo al label \n");
		printf("	Yendo al label con pos: %i, con nombre: %s, \n",pos_etiqueta, etiq_sin_blancos);
	}else{
		txt_write_in_file(cpu_file_log, "Hubo un error en la busqueda de la etiqueta\n");
		printf("	ERROR en la busqueda de la etiqueta:%s \n",etiq_sin_blancos);
		finalizarContexto(ERROR);
	}
	free(etiq_sin_blancos);
}

void llamarSinRetorno(t_nombre_etiqueta etiqueta){
	int32_t pos_retorno;
	/*Preserva el contexto de ejecución actual para poder retornar luego al mismo.*/
	preservarContexto();

	//actualizo las estructuras
	pcb->dir_cont_actual = pcb->dir_cont_actual + (5*pcb->cant_var_cont_actual)+8;//todo probar q ande
	pcb->cant_var_cont_actual = 0;
	regenerar_dicc_var();
	pos_retorno = metadata_buscar_etiqueta(etiqueta, etiquetas, pcb->tam_indice_etiquetas);
	pcb->program_counter = pos_retorno;

	txt_write_in_file(cpu_file_log, "Llamando sin retorno a etiqueta\n");
	printf("	Llamando sin retorno a etiqueta:%s, con pos_PC:%i\n", etiqueta, pos_retorno);
}

void llamarConRetorno(t_nombre_etiqueta etiqueta, t_puntero donde_retornar){
	int32_t base, offset, tam, pos_retorno;
	//preservar el contexto, agrega al segmento stack la dir de memoria del contexto anterior y la cantidad de variables del contex anterior
	preservarContexto();
	//aca le digo a la umv q me guarde en el stack la dir de la variable a la q tengo q asignarle el valor retornado por la "funcion"

	base = pcb->dir_seg_stack;
	offset = pcb->dir_cont_actual - base + (pcb->cant_var_cont_actual*5)+8;

	tam = sizeof(int32_t);

	char *buffer = copiar_int_to_buffer(donde_retornar);
	enviar_men_cpu_umv_destruir(ALM_BYTES, base, offset, tam, buffer);
	free(buffer);

	t_men_comun *r_alm = socket_recv_comun(socketUmv);
	if(r_alm->tipo == R_ALM_BYTES){
		txt_write_in_file(cpu_file_log, "Se almaceno la direccion de retorno \n");
		printf("	Se almaceno la direccion de retorno\n");

		//actualizo las estructuras
		pcb->dir_cont_actual = pcb->dir_seg_stack+offset+4;
		pcb->cant_var_cont_actual = 0;
		regenerar_dicc_var();
		pos_retorno = metadata_buscar_etiqueta(etiqueta, etiquetas, pcb->tam_indice_etiquetas);
		pcb->program_counter = pos_retorno;

		txt_write_in_file(cpu_file_log, "Llamando con retorno a la etiqueta\n");
		printf("	Llamando con retorno a etiqueta %s, pos_PC:%i\n", etiqueta, pos_retorno);
		return;
	}
	if(r_alm->tipo == SEGMEN_FAULT){
		finalizarContexto(SEGMEN_FAULT);
		return;
	}
	printf("	ERROR tipo de dato:%i, recibido es erroneo\n", r_alm->tipo);
}

void finalizar(){
	int32_t base, offset, tam;

	//si es el fin de programa, le digo al kernel q finalizo y salgo de la funcion
	if(pcb->dir_cont_actual == pcb->dir_seg_stack){
		finalizarContexto(OK);
		txt_write_in_file(cpu_file_log, "Finalizando el programa actual\n");
		printf("	Finalizando el programa actual\n");
		return;
	}

	int32_t prog_counter;
	int32_t dir_context_ant;
	//consigo 2 int, el primero para el program counter a retornar y el segundo para la dir de memoria al contexto anterior
	tam= 8;
	base= pcb->dir_seg_stack;
	offset= pcb->dir_cont_actual-base- 8;

	enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, tam, NULL);

	t_men_comun *rec_ret= socket_recv_comun(socketUmv);
	if(rec_ret->tipo == R_SOL_BYTES){
		memcpy(&dir_context_ant, rec_ret->dato,sizeof(int32_t));
		memcpy(&prog_counter, rec_ret->dato+4,sizeof(int32_t));
	}else{
		finalizarContexto(SEGMEN_FAULT);
		return;
	}

	//actualizar el pcb
	int32_t cant_var = (pcb->dir_cont_actual - dir_context_ant-8)/5;

	actualizar_pcb(cant_var,prog_counter,dir_context_ant);

	regenerar_dicc_var();
}

void retornar(t_valor_variable retorno){
	int32_t base, offset, tam;
	int32_t dir_retorno;
	int32_t prog_counter;
	int32_t dir_context_ant;

	//consigo 3 int, el primero para asignarle el valor retorno, el segundo para el program counter a retornar y el tercero para la dir de memoria al contexto anterior
	tam= 12;
	base= pcb->dir_seg_stack;
	offset= pcb->dir_cont_actual-base- 12;
	enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, tam, NULL);

	t_men_comun *rec_ret= socket_recv_comun(socketUmv);
	if(rec_ret->tipo == R_SOL_BYTES){
		memcpy(&dir_context_ant, rec_ret->dato,sizeof(int32_t));
		memcpy(&prog_counter, rec_ret->dato+4,sizeof(int32_t));
		memcpy(&dir_retorno, rec_ret->dato+8,sizeof(int32_t));
	}else{
		finalizarContexto(SEGMEN_FAULT);
		return;
	}

	//actualizar el pcb
	int32_t cant_var = (pcb->dir_cont_actual - dir_context_ant-12)/5;
	actualizar_pcb(cant_var,prog_counter,dir_context_ant);

	regenerar_dicc_var();

	//almaceno la variable retorno en el contexto anterior
	char *buffer = copiar_int_to_buffer(retorno);

	offset = (dir_retorno - base + 1);

	enviar_men_cpu_umv_destruir(ALM_BYTES, base, offset, sizeof(int32_t), buffer);

	free(buffer);
	t_men_comun *rec_alm = socket_recv_comun(socketUmv);
	if(rec_alm->tipo == R_ALM_BYTES){
		txt_write_in_file(cpu_file_log, "Retornando valor de variable\n");
		printf("	Retornando valor de variable:%d en la direccion %d\n", retorno, dir_retorno);
	}else{
		printf("	ERROR se esperaba un tipo de dato R_SOL_BYTES y recibo un:%i\n", rec_ret->tipo);
	}
}

void imprimir(t_valor_variable valor_mostrar){
	if(!huboSegFault){
		char *string_int = string_itoa(valor_mostrar);

		enviar_men_comun_destruir(socketKernel, IMPRIMIR_VALOR, string_int, string_length(string_int)+BARRA_CERO);
		char *aux_string = string_itoa(pcb->id);
		enviar_men_comun_destruir(socketKernel, ID_PROG, aux_string, string_length(aux_string)+BARRA_CERO);
		free(aux_string);
		recibir_resp_kernel(R_IMPRIMIR);

		txt_write_in_file(cpu_file_log, "	Imprimiendo valor\n");
		printf("	Imprimiendo valor: %s\n", string_int);
		free(string_int);
	}
}

void imprimirTexto(char* texto){
	char *texto_sin_esc = sacar_caracteres_escape(texto);

	enviar_men_comun_destruir(socketKernel, IMPRIMIR_TEXTO, texto_sin_esc, string_length(texto_sin_esc)+BARRA_CERO);

	char *aux_string = string_itoa(pcb->id);
	enviar_men_comun_destruir(socketKernel, ID_PROG, aux_string, string_length(aux_string)+BARRA_CERO);
	free(aux_string);

	recibir_resp_kernel(R_IMPRIMIR);

	txt_write_in_file(cpu_file_log, "	Imprimiendo texto\n");
	printf("	Imprimiendo texto: %s\n", texto_sin_esc);
	free(texto_sin_esc);
}

void entradaSalida(t_nombre_dispositivo dispositivo, int tiempo){
	enviar_men_comun_destruir(socketKernel, IO_ID, dispositivo, string_length(dispositivo)+BARRA_CERO);
	char *aux_tiempo = string_itoa(tiempo);
	enviar_men_comun_destruir(socketKernel, IO_CANT_UNIDADES,aux_tiempo , string_length(aux_tiempo)+BARRA_CERO);
	free(aux_tiempo);

	enviar_pcb_destruir();
	fueFinEjecucion = 1;
	entre_io =1;

	txt_write_in_file(cpu_file_log, "Saliendo a IO en dispositivo\n");
	printf("	Saliendo a IO ID:%s, cant. unidades:%d\n", dispositivo, tiempo);
}

void wait(t_nombre_semaforo identificador_semaforo){
	enviar_men_comun_destruir(socketKernel, WAIT, identificador_semaforo, string_length(identificador_semaforo)+BARRA_CERO);
	txt_write_in_file(cpu_file_log, "Haciendo wait a semaforo\n");
	printf("	Haciendo wait a semaforo %s\n", identificador_semaforo);

	t_men_comun *men_resp = socket_recv_comun(socketKernel);

	switch(men_resp->tipo){
		case SEM_OK:
			//El semaforo esta disponible, seguir procesando o lo que sea
			printf("	Se recibio el mensaje SEM_OK, se continuara con la ejecucion\n");
			txt_write_in_file(cpu_file_log, "El semaforo esta disponible, se continua con la ejecucion.");
			//No hago nada, continua con la ejecucion normalmente
			break;
		case SEM_BLOQUEADO:
			finalizarContexto(SEM_BLOQUEADO); // para que no busque la proxima instruccion, y se quede bloqueado
			// no hace falta mandar nada al kernel, ya que al avisar que lo bloquea lo manda a bloqueado por get_cpu
			printf("	Se recibio el mensaje SEM_BLOQUEADO del semaforo,se bloquea el programa n° %i\n", pcb->id);
			txt_write_in_file(cpu_file_log, "Se bloqueo el programa por un semaforo, se desaloja de la cpu.");
			//El semaforo esta bloqueado, desalojar(suponiendo que tiene que cambiar de proceso, si es asi, mandar el pcb)
			break;
		case SEM_INEX:
			printf("	Acceso a semaforo %s inexistente \n",identificador_semaforo);
			txt_write_in_file(cpu_file_log,"Acceso a semaforo inexistente");
			finalizarContexto(SEM_INEX);
			break;
		default:
			printf("	El tipo de dato recibido: %i es erroneo\n",men_resp->tipo);
			txt_write_in_file(cpu_file_log,"Se recibio un msj del kernel de tipo erroneo");
			break;
	}
	destruir_men_comun(men_resp);
}

void mi_signal(t_nombre_semaforo identificador_semaforo){
	enviar_men_comun_destruir(socketKernel, SIGNAL, identificador_semaforo, string_length(identificador_semaforo)+BARRA_CERO);

	txt_write_in_file(cpu_file_log,"Haciendo signal a semaforo\n");
	printf("	Haciendo signal a semaforo %s\n", identificador_semaforo);

	t_men_comun *respuesta = socket_recv_comun(socketKernel);

	if(respuesta->tipo == SEM_INEX){
		txt_write_in_file(cpu_file_log,"Signal a semaforo inexistente\n");
		printf("	Signal a semaforo %s inexistente \n", identificador_semaforo);
		finalizarContexto(SEM_INEX);
	}
	destruir_men_comun(respuesta);
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

void handshake_umv(char *puerto, char *ip){
	//envio a la UMV
	socketUmv = socket_crear_client(puerto,ip);

	enviar_men_comun_destruir(socketUmv, HS_CPU, NULL, 0);

	//espero coneccion de la UMV
	t_men_comun *mensaje_inicial = socket_recv_comun(socketUmv);
	if(mensaje_inicial->tipo == HS_UMV){
		txt_write_in_file(cpu_file_log, "UMV conectada \n");
		printf("UMV conectada\n");
		destruir_men_comun(mensaje_inicial);
	}else{
		txt_write_in_file(cpu_file_log, "ERROR HANDSHAKE UMV\n");
		printf("ERROR HANDSHAKE\n");
	}
}

void handshake_kernel(char *puerto, char *ip){
	//envio al kernel
	socketKernel = socket_crear_client(puerto,ip);

	enviar_men_comun_destruir(socketKernel, HS_CPU, NULL, 0);

	//espero conexion de kernel
	t_men_comun *mensaje_inicial = socket_recv_comun(socketKernel);
	if(mensaje_inicial->tipo == HS_KERNEL){
		txt_write_in_file(cpu_file_log, "Kernel conectado\n");
		printf("Kernel conectado\n");
		destruir_men_comun(mensaje_inicial);
	}else{
		txt_write_in_file(cpu_file_log, "ERROR HANDSHAKE KERNEL \n");
		printf("ERROR HANDSHAKE\n");
	}
}

void imprimo_config(char *puertoKernel, char *ipKernel, char *puertoUmv, char *ipUmv){
	printf("\n\n------------------------------Archivo Config----------------------\n");
	printf("	Puerto Kernel	= %s\n", puertoKernel);
	printf("	IP Kernel	= %s\n", ipKernel);
	printf("	Puerto UMV	= %s\n", puertoUmv);
	printf("	IP UMV		= %s\n", ipUmv);
	printf("------------------------------------------------------------------\n\n");
}

void destruir_dic_Variables(){
	void _liberar_pos(int32_t *pos){
		free(pos);
	}
	dictionary_destroy_and_destroy_elements(dic_Variables, (void *)_liberar_pos);
}

void cambio_PA(int32_t id_proc){
	printf("Cambio el proceso activo al proceso nº%i\n",id_proc);
	enviar_men_cpu_umv_destruir(CAMBIO_PA, id_proc, 0, 0, NULL);
}
//recibe un int y lo transforma en un char *
char *copiar_int_to_buffer(int32_t un_int){
	char *ret = malloc(sizeof(int32_t));
	memcpy(ret, &un_int, sizeof(int32_t));
	return ret;
}
/*envia un mensaje cpu umv y lo destruye */
void enviar_men_cpu_umv_destruir(int32_t tipo, int32_t base, int32_t offset, int32_t tam, char *buffer){
	t_men_cpu_umv *men_cpu_umv = crear_men_cpu_umv(tipo, base, offset,tam , buffer);
	socket_send_cpu_umv(socketUmv, men_cpu_umv);
	destruir_men_cpu_umv(men_cpu_umv);
}

void recibir_resp_kernel(int32_t tipo_esperado){
	t_men_comun *men_sincro_ok = socket_recv_comun(socketKernel);
	if(men_sincro_ok->tipo != tipo_esperado)
		printf("	ERROR el kernel me mando:%i y se esperaba %i\n",men_sincro_ok->tipo,tipo_esperado);
	destruir_men_comun(men_sincro_ok);
}

void limpiar_destruir_dic_var(){
	void _liberar_var(int32_t *pos){
		free(pos);
	}
	dictionary_clean_and_destroy_elements(dic_Variables, (void *)_liberar_var);
}

void regenerar_dicc_var(){
	int32_t base, offset,i;
	char aux_char;

	limpiar_destruir_dic_var();
	base= pcb->dir_seg_stack;

	for(i=0;i<pcb->cant_var_cont_actual;i++){
		offset= pcb->dir_cont_actual- base + (i*5);

		enviar_men_cpu_umv_destruir(SOL_BYTES, base, offset, 1, NULL);

		t_men_comun *men_var = socket_recv_comun(socketUmv);

		int32_t *pos = malloc(sizeof(int32_t));
		*pos = base + offset;
		aux_char = es_numero_pasar_char(men_var->dato[0]);
		char *key = string_substring_until(&aux_char, 1);

		dictionary_put(dic_Variables, key, pos );
		free(key);
		destruir_men_comun(men_var);
	}
}

void actualizar_pcb(int32_t cant_var,int32_t program_counter,int32_t dir_primer_byte_umv){
	pcb->cant_var_cont_actual = cant_var;
	pcb->program_counter=program_counter;
	pcb->dir_cont_actual=dir_primer_byte_umv;
}

char *sacar_caracteres_escape(char *un_string){
	char *aux_string = string_duplicate(un_string);
	string_trim(&aux_string);

	return aux_string;
}
