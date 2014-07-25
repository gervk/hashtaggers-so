#include "kernel.h"

fd_set conj_soc_progs;   // conjunto maestro de sockets del PLP(sockets de progrmas)
fd_set conj_soc_cpus;    // conjunto maestro de sockets del PCP(sockets de cpus)
int32_t soc_umv;
t_colas *colas;  //colas de los cinco estados de los procesos
t_queue *cola_cpu; //cola donde tengo los cpu conectados
pthread_mutex_t mutex_new = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_ready = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_block = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_exec = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_exit = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_uso_cola_cpu= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_io= PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_dicc_var= PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_dicc_sem= PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t mutex_dispositivos_io= PTHREAD_MUTEX_INITIALIZER;

sem_t cant_new, cant_ready, cant_exit;
sem_t cant_multiprog, cant_cpu_libres;
int32_t quit_sistema = 1, _QUANTUM_MAX, _RETARDO;
t_dictionary *dicc_disp_io;
t_dictionary *dicc_var;
t_dictionary *dicc_sem;
FILE *plp_log;
FILE *pcp_log;


int main(void){
	dicc_var = dictionary_create();
	dicc_sem = dictionary_create();
	//diccionario dispositivos
	dicc_disp_io = dictionary_create();

	t_datos_config *diccionario_config = levantar_config();

	plp_log = txt_open_for_append("./kernel/logs/plp.log");
	txt_write_in_file(plp_log,"---------------------Nueva ejecucion------------------------------\n");

	pcp_log = txt_open_for_append("./kernel/logs/pcp.log");
	txt_write_in_file(pcp_log,"---------------------Nueva ejecucion------------------------------\n");

	//Inicializacion semaforos
	crear_cont(&cant_new , 0);
	crear_cont(&cant_ready , 0);
	crear_cont(&cant_exit , 0);

	crear_cont(&cant_cpu_libres , 0);
	crear_cont(&cant_multiprog , diccionario_config->multiprogramacion); //cantidad de procesos que todavia entran

	//Creacion de colas
	cola_cpu = queue_create();
	colas = malloc(sizeof(t_colas));
	colas->cola_new = queue_create();
	colas->cola_ready = queue_create();
	colas->cola_block = queue_create();
	colas->cola_exec = queue_create();
	colas->cola_exit = queue_create();

	//parametros plp pcp
	t_param_plp *param_plp = ini_pram_plp(diccionario_config);
	t_param_pcp *param_pcp = ini_pram_pcp(diccionario_config);

	//Creacion de hilos
	pthread_t hilo_imp_colas, hilo_plp, hilo_pcp;
	pthread_create(&hilo_imp_colas, NULL, imp_colas , NULL);
	pthread_create(&hilo_plp, NULL, plp, (void *)param_plp);
	pthread_create(&hilo_pcp, NULL, pcp, (void *)param_pcp);
	pthread_join(hilo_plp, NULL);
	pthread_join(hilo_pcp, NULL);

	free(param_plp->puerto_prog);
	free(param_plp->puerto_umv);
	free(param_plp->ip_umv);
	free(param_plp);
	free(param_pcp->puerto_cpu);
	queue_destroy(colas->cola_block);
	queue_destroy(colas->cola_exec);
	queue_destroy(colas->cola_exit);
	queue_destroy(colas->cola_new);
	queue_destroy(colas->cola_ready);
	queue_destroy(cola_cpu);
	return EXIT_SUCCESS;
}

void *plp(t_param_plp *param_plp){

	//Creacion del hilo que maneja el pasaje de pcbs de new a ready
	pthread_t hilo_new_ready;
	pthread_create(&hilo_new_ready, NULL, manejador_new_ready, NULL);

	int32_t contador_prog = 0;
	fd_set read_fds; // conjunto temporal de sockets para select()
	int32_t fdmax;   // socket con el mayor valor

	//Conecta con la umv
	handshake_umv(param_plp->ip_umv, param_plp->puerto_umv);

	//Abre un server para escuchar las conexiones de los programas
	int32_t listener_prog = socket_crear_server(param_plp->puerto_prog);
	txt_write_in_file(plp_log,"Escuchando conexiones entrantes de programas \n");
    int32_t prog_new_fd;

	FD_ZERO(&conj_soc_progs);    // borra los conjuntos maestro y temporal de sockets
	FD_ZERO(&read_fds);
	FD_SET(listener_prog, &conj_soc_progs);

	fdmax = listener_prog; // por ahora es éste

	int32_t i;

	while(quit_sistema){

		read_fds = conj_soc_progs; // Copia el conjunto maestro al temporal

		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
			txt_write_in_file(plp_log,"Error en el select \n");
			perror("PLP-select");
			exit(1);
		}

		for(i = 3; i <= fdmax; i++) {
			if (FD_ISSET(i, &read_fds)) { // Si hay datos entrantes en el socket i

				if (i == listener_prog) { //Si i es el socket que espera conexiones, es porque hay un nuevo prog

					prog_new_fd = socket_accept(listener_prog);
					handshake_prog(prog_new_fd);

					txt_write_in_file(plp_log,"Se acepto la conexion de un nuevo programa con socket n°:");
					logear_int(plp_log,prog_new_fd);
					txt_write_in_file(plp_log,"\n");

					FD_SET(prog_new_fd, &conj_soc_progs);

					if (prog_new_fd > fdmax) // Actualiza el socket maximo
						fdmax = prog_new_fd;

				}else{

					// Sino, es el socket de un programa ya conectado que manda datos
					t_men_comun *men_cod_prog = socket_recv_comun(i);

					if (men_cod_prog->tipo == CONEC_CERRADA) {
						txt_write_in_file(plp_log,"Cerro la conexion el programa con socket n°:");
						logear_int(plp_log,i);
						txt_write_in_file(plp_log,"\n");
						//printf("PLP-select: Prog desconectado n°socket %d\n", i);
						FD_CLR(i, &conj_soc_progs); // Elimina al socket del conjunto maestro
						destruir_men_comun(men_cod_prog);

						t_cpu *aux_cpu=get_cpu_soc_prog(i);
						if (aux_cpu==NULL)
							continue;
						socket_cerrar(i);
						aux_cpu->id_prog_exec = 0;

						pthread_mutex_lock(&mutex_uso_cola_cpu);
						queue_push(cola_cpu,aux_cpu);
						sem_incre(&cant_cpu_libres);
						pthread_mutex_unlock(&mutex_uso_cola_cpu);
						continue;
					}

					//printf("PLP-select: nuevo prog con socket n°%i\n", i);
					txt_write_in_file(plp_log,"Nuevo programa con socket n°:");
					logear_int(plp_log,i);
					txt_write_in_file(plp_log,"\n");

					contador_prog++;

					//Pide mem para el prog
					t_resp_sol_mem *resp_sol = solicitar_mem(men_cod_prog, param_plp->tam_stack,contador_prog);

					if (resp_sol->memoria_insuficiente == MEM_OVERLOAD){
						txt_write_in_file(plp_log,"Memoria insuficiente para el programa con socket n°:");
						logear_int(plp_log,i);
						txt_write_in_file(plp_log,"\n");

						//Avisa al programa que no hay memoria
						char *aux_sring = string_itoa(contador_prog);
						enviar_men_comun_destruir(i,MEM_OVERLOAD,aux_sring,string_length(aux_sring)+1);
						FD_CLR(i, &conj_soc_progs);
						socket_cerrar(i);
						free(aux_sring);
						free(resp_sol);
					}else{

						//Crea el pcb y lo pone en new
						t_pcb_otros *pcb_otros = malloc(sizeof(t_pcb_otros));
						pcb_otros->n_socket = i;
						pcb_otros->pcb = crear_pcb_escribir_seg_UMV(men_cod_prog,resp_sol, contador_prog);
						free(resp_sol);
						pcb_otros->peso = calcular_peso(men_cod_prog);
						pcb_otros->tipo_fin_ejecucion = -1;
						pthread_mutex_lock(&mutex_new);
						queue_push(colas->cola_new,pcb_otros);
						sem_incre(&cant_new);
						pthread_mutex_unlock(&mutex_new);

						txt_write_in_file(plp_log,"Se creo el pcb para el programa con socket n°:");
						logear_int(plp_log,i);
						txt_write_in_file(plp_log," , su id es:");
						logear_int(plp_log,pcb_otros->pcb->id);
						txt_write_in_file(plp_log,"\n");
					}
					destruir_men_comun(men_cod_prog);
				}
			}
		}
	usleep(_RETARDO*1000);
	}
	return NULL;
}

void *pcp(t_param_pcp *param_pcp){
	int32_t i, fdmax, cpu_new_fd;
	fd_set read_fds;

	// Crea el hilo que pasa los pcb de ready a exec
	pthread_t hilo_ready_exec;
	pthread_create(&hilo_ready_exec, NULL, manejador_ready_exec,NULL);

	// Crea el hilo que maneja las llegadas de pcbs a exit
	pthread_t hilo_exit;
	pthread_create(&hilo_exit, NULL, manejador_exit, NULL);

	int32_t listener_cpu = socket_crear_server(param_pcp->puerto_cpu);
	txt_write_in_file(pcp_log,"Escuchando conexiones entrantes de cpus \n");

	FD_ZERO(&conj_soc_cpus);
	FD_ZERO(&read_fds);
	FD_SET(listener_cpu, &conj_soc_cpus);
	fdmax = listener_cpu;

	while(quit_sistema){

		read_fds = conj_soc_cpus;

		if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
			txt_write_in_file(pcp_log,"Error en el select \n");
			perror("PCP-select");
			exit(1);
		}

		for(i = 3; i <= fdmax; i++) {

			if (FD_ISSET(i, &read_fds)) {

				if (i == listener_cpu) { // Si los datos son en el srv que escucha cpus(se conecta una nueva cpu)
					cpu_new_fd = socket_accept(listener_cpu);
					handshake_cpu(cpu_new_fd);

					txt_write_in_file(pcp_log,"Se acepto la conexion de una nueva CPU con socket n°:");
					logear_int(plp_log,cpu_new_fd);
					txt_write_in_file(pcp_log,"\n");

					t_cpu *cpu = malloc(sizeof(t_cpu));

					// Arma estructura con el n° de socket del cpu y el id del prog que esta ejecutando
					cpu->soc_cpu = cpu_new_fd;
					cpu->id_prog_exec = 0;

					//printf("PCP-select: CPU conectada n°socket %i\n", cpu_new_fd);
					FD_SET(cpu_new_fd, &conj_soc_cpus);

					// Pone la estructura en la cola de cpus (cola con cpus libres)
					pthread_mutex_lock(&mutex_uso_cola_cpu);
					queue_push(cola_cpu, cpu);
					sem_incre(&cant_cpu_libres);
					pthread_mutex_unlock(&mutex_uso_cola_cpu);

					if (cpu_new_fd > fdmax)
						fdmax = cpu_new_fd;

				}else{

					// Sino, es un cpu mandando datos
					t_men_comun *men_cpu = socket_recv_comun(i);
					switch(men_cpu->tipo){
					case CONEC_CERRADA:
						conec_cerrada_cpu(i, men_cpu);
						break;
					case SIGUSR1_CPU_DESCONEC:
						manejador_sigusr1(i, men_cpu);
						break;
					case FIN_QUANTUM:
						fin_quantum(i);
						destruir_men_comun(men_cpu);
						break;
					case FIN_EJECUCION:
						fin_ejecucion(FIN_EJECUCION,i);
						destruir_men_comun(men_cpu);
						break;
					case SEGMEN_FAULT:
						fin_ejecucion(SEGMEN_FAULT,i);
						destruir_men_comun(men_cpu);
						break;
					case IMPRIMIR_TEXTO:
						imprimir_texto(i, men_cpu);
						break;
					case IMPRIMIR_VALOR:
						imprimir_valor(i, men_cpu);
						break;
					case OBTENER_VALOR:
						obtener_valor_compartida(i, men_cpu);
						break;
					case GRABAR_VALOR:
						grabar_valor_compartida(i, men_cpu);
						break;
					case WAIT:
						wait(i ,men_cpu);
						break;
					case SIGNAL:
						signal(i, men_cpu);
						break;
					case IO_ID:
						enviar_IO(i, men_cpu->dato);
						destruir_men_comun(men_cpu);
						break;
					default:
						printf("ERROR se esperaba al recibir el tipo de dato:%i\n", men_cpu->tipo);
						break;
					}
				}
			}
		}
		usleep(_RETARDO*1000);
	}
	return NULL;
}

void wait(int32_t soc_cpu,t_men_comun *men_cpu){
	t_semaforo *semaforo;
	t_cpu *aux_cpu;
	t_pcb_otros *aux_pcb_otros;

	pthread_mutex_lock(&mutex_dicc_sem);

	semaforo = dictionary_get(dicc_sem,men_cpu->dato);

	if(semaforo != NULL){
		if(semaforo->valor > 0){
			(semaforo->valor)--;
			enviar_men_comun_destruir(soc_cpu, SEM_OK, NULL, 0);
		}else{
			(semaforo->valor)--;
			aux_cpu = get_cpu(soc_cpu);

			enviar_men_comun_destruir(soc_cpu, SEM_BLOQUEADO, NULL, 0);

			txt_write_in_file(pcp_log,"Se bloqueo al programa por un wait, con id ");
			logear_int(pcp_log,aux_cpu->id_prog_exec);
			txt_write_in_file(pcp_log,"/n");

			aux_pcb_otros = actualizar_pcb_y_bloq(aux_cpu);

			queue_push(semaforo->procesos,aux_pcb_otros);
		}

		txt_write_in_file(pcp_log,"WAIT por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");
	}else{
		llamada_erronea(soc_cpu, SEM_INEX);

		txt_write_in_file(pcp_log,"Error en WAIT (semaforo inexistente) por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");
	}

	pthread_mutex_unlock(&mutex_dicc_sem);

	destruir_men_comun(men_cpu);
}

void signal(int32_t soc_cpu,t_men_comun *men_cpu){
	t_semaforo *semaforo;
	t_pcb_otros *aux_pcb_otros;

	pthread_mutex_lock(&mutex_dicc_sem);

	semaforo = dictionary_get(dicc_sem,men_cpu->dato);

	if(semaforo != NULL){
		(semaforo->valor)++;

		enviar_men_comun_destruir(soc_cpu, SEM_OK, NULL, 0);

		if(queue_size(semaforo->procesos)>0){
			aux_pcb_otros = queue_pop(semaforo->procesos);
			pasar_pcbBlock_ready(aux_pcb_otros->pcb->id);
		}

		txt_write_in_file(pcp_log,"SIGNAL por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");

	}else{
		llamada_erronea(soc_cpu, SEM_INEX);

		txt_write_in_file(pcp_log,"Error en SIGNAL (semaforo inexistente) por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");
	}

	pthread_mutex_unlock(&mutex_dicc_sem);

	destruir_men_comun(men_cpu);
}

void manejador_sigusr1(int32_t soc_cpu,t_men_comun *men_cpu){
	t_cpu *aux_cpu;
	t_pcb_otros *aux_pcb_otros;
	FD_CLR(soc_cpu, &conj_soc_cpus);

	txt_write_in_file(pcp_log,"Cerro la conexion con la senial SIGUSR1 el cpu con socket n°:");
	logear_int(pcp_log,soc_cpu);
	txt_write_in_file(pcp_log,"\n");
	//printf("PCP-select: CPU desconectada con SIGUSR1 socket n°%i\n", soc_cpu);
	// Agarra el cpu a partir de su n° de socket
	aux_cpu = get_cpu(soc_cpu);

	if (aux_cpu->id_prog_exec != 0){
		// Actualizacion pcb
		aux_pcb_otros=get_pcb_otros_exec(aux_cpu->id_prog_exec);
		t_men_quantum_pcb *men_quantum_pcb = socket_recv_quantum_pcb(soc_cpu);
		actualizar_pcb(aux_pcb_otros->pcb,men_quantum_pcb->pcb);
		destruir_quantum_pcb(men_quantum_pcb);
		// Pone el pcb en ready
		pthread_mutex_lock(&mutex_ready);
		queue_push(colas->cola_ready,aux_pcb_otros);
		sem_incre(&cant_ready);
		pthread_mutex_unlock(&mutex_ready);
	}else{
		sem_decre(&cant_cpu_libres);
	}
	free(aux_cpu);

	socket_cerrar(soc_cpu);
	destruir_men_comun(men_cpu);
}

void conec_cerrada_cpu(int32_t soc_cpu,t_men_comun *men_cpu){
	t_cpu *aux_cpu;
	t_pcb_otros *aux_pcb_otros;

	txt_write_in_file(pcp_log,"Cerro la conexion el cpu con socket n°:");
	logear_int(pcp_log,soc_cpu);
	txt_write_in_file(pcp_log,"\n");
	//printf("PCP-select: CPU desconectada n°socket %i\n", soc_cpu);

	// Agarra el cpu a partir de su n° de socket
	aux_cpu = get_cpu(soc_cpu);
	if (aux_cpu->id_prog_exec != 0){
		// Saca el pcb que corresponde de la cola de ejec a partir del id
		aux_pcb_otros = get_pcb_otros_exec(aux_cpu->id_prog_exec);
		aux_pcb_otros->tipo_fin_ejecucion= CPU_DESCONEC;
		// Pone el pcb en exit
		pasar_pcb_exit(aux_pcb_otros);
	}else{
		sem_decre(&cant_cpu_libres);
	}
	free(aux_cpu);
	FD_CLR(soc_cpu, &conj_soc_cpus);
	socket_cerrar(soc_cpu);
	destruir_men_comun(men_cpu);
}

void enviar_IO(int32_t soc_cpu, char *id_IO){
	t_men_comun *men;
	t_IO *aux_IO;

	t_cpu *aux_cpu = get_cpu(soc_cpu);

	men = socket_recv_comun(soc_cpu);
	int32_t cant_unidades = atoi(men->dato);

	//Crea la estructura que tiene el id del programa y la cantidad de unidades que quiere usar para ponerlo en la cola del dispositivo
	t_IO_espera *espera = malloc(sizeof(t_IO_espera));
	espera->id_prog = aux_cpu->id_prog_exec;
	espera->unidades = cant_unidades;

	pthread_mutex_lock(&mutex_io);

	aux_IO = dictionary_get(dicc_disp_io, id_IO);
	queue_push(aux_IO->procesos, espera);
	dictionary_put(dicc_disp_io, id_IO, aux_IO);

	actualizar_pcb_y_bloq(aux_cpu);

	sem_incre(&(aux_IO->cont_cant_proc));

	pthread_mutex_unlock(&mutex_io);

	destruir_men_comun(men);
}

void fin_quantum(int32_t soc_cpu){
	t_pcb_otros *aux_pcb_otros;
	t_cpu *aux_cpu = get_cpu(soc_cpu);

	// Actualizacion pcb
	aux_pcb_otros=get_pcb_otros_exec(aux_cpu->id_prog_exec);
	t_men_quantum_pcb *men_quantum_pcb = socket_recv_quantum_pcb(soc_cpu);

	actualizar_pcb(aux_pcb_otros->pcb,men_quantum_pcb->pcb);
	destruir_quantum_pcb(men_quantum_pcb);

	// Setea en la estructura del cpu como que no esta ejecutando nada
	aux_cpu->id_prog_exec = 0;
	aux_cpu->soc_prog = 0;

	// Pone el pcb en ready

	pthread_mutex_lock(&mutex_ready);
	pthread_mutex_lock(&mutex_uso_cola_cpu);

	queue_push(cola_cpu,aux_cpu);
	queue_push(colas->cola_ready,aux_pcb_otros);

	sem_incre(&cant_ready);
	sem_incre(&cant_cpu_libres);

	pthread_mutex_unlock(&mutex_uso_cola_cpu);
	pthread_mutex_unlock(&mutex_ready);

	txt_write_in_file(pcp_log,"Termino un quantum del programa:");
	logear_int(pcp_log,aux_pcb_otros->pcb->id);
	txt_write_in_file(pcp_log,"\n");
}

void obtener_valor_compartida(int32_t soc_cpu, t_men_comun *men_obt_valor){
	int32_t *valor;
	char *string_valor;
	char *key = men_obt_valor->dato;

	pthread_mutex_lock(&mutex_dicc_var);

	if (dictionary_has_key(dicc_var, key)){
		valor = dictionary_get(dicc_var, key);
		string_valor = string_itoa(valor);
		enviar_men_comun_destruir(soc_cpu ,R_OBTENER_VALOR,string_valor,string_length(string_valor)+1);
		txt_write_in_file(pcp_log,"OBTENER VALOR por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");
	}else{
		llamada_erronea(soc_cpu, VAR_INEX);
		txt_write_in_file(pcp_log,"Error en OBTENER VALOR (var inexistente) por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");
	}

	pthread_mutex_unlock(&mutex_dicc_var);

	destruir_men_comun(men_obt_valor);
}

void grabar_valor_compartida(int32_t soc_cpu, t_men_comun *men_gra_valor){
	int32_t valor;
	char *key = men_gra_valor->dato;
	t_men_comun *men_valor;

	pthread_mutex_lock(&mutex_dicc_var);

	if (dictionary_has_key(dicc_var, key)){

		enviar_men_comun_destruir(soc_cpu,R_GRABAR_VALOR,NULL,0);
		men_valor = socket_recv_comun(soc_cpu);

		if((men_valor->tipo) != VALOR_ASIGNADO)
			printf("ERROR: esperaba recibir VALOR_ASIGNADO y recibi: %i",men_valor->tipo);

		dictionary_remove(dicc_var,key);
		valor = atoi(men_valor->dato);
		dictionary_put(dicc_var,key, valor);
		txt_write_in_file(pcp_log,"GRABAR VALOR por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");
		destruir_men_comun(men_valor);

		enviar_men_comun_destruir(soc_cpu,R_GRABAR_VALOR,NULL,0);
	}else{
		llamada_erronea(soc_cpu, VAR_INEX);
		txt_write_in_file(pcp_log,"Error en GRABAR VALOR (var inexistente) por cpu con socket n°:");
		logear_int(pcp_log,soc_cpu);
		txt_write_in_file(pcp_log,"\n");
	}

	pthread_mutex_unlock(&mutex_dicc_var);

	destruir_men_comun(men_gra_valor);
}

void llamada_erronea(int32_t soc_cpu,int32_t tipo_error){
	t_cpu *aux_cpu = get_cpu(soc_cpu);
	t_pcb_otros *aux_pcb= get_pcb_otros_exec(aux_cpu->id_prog_exec);

	aux_cpu->id_prog_exec = 0;
	aux_cpu->soc_prog = 0;

	aux_pcb->tipo_fin_ejecucion=tipo_error;

	pasar_pcb_exit(aux_pcb);

	enviar_men_comun_destruir(soc_cpu,tipo_error,NULL,0);

	pthread_mutex_lock(&mutex_uso_cola_cpu);
	queue_push(cola_cpu,aux_cpu);
	sem_incre(&cant_cpu_libres);
	pthread_mutex_unlock(&mutex_uso_cola_cpu);
}

void imprimir_valor(int32_t soc, t_men_comun *men_imp_valor){
	t_men_comun *men_id_prog;
	men_id_prog = socket_recv_comun(soc);

	if (men_id_prog->tipo != ID_PROG)
		printf("ERROR: esperaba el id de un programa y recibi: %i\n",men_id_prog->tipo);

	t_cpu *aux_cpu = get_cpu(soc);

	socket_send_comun(aux_cpu->soc_prog, men_imp_valor);

	pthread_mutex_lock(&mutex_uso_cola_cpu);
	queue_push(cola_cpu,aux_cpu);
	pthread_mutex_unlock(&mutex_uso_cola_cpu);

	txt_write_in_file(pcp_log,"IMPRIMIR_VALOR por cpu con socket");
	logear_int(pcp_log,soc);
	txt_write_in_file(pcp_log,"al programa con id ");
	logear_int(pcp_log,aux_cpu->id_prog_exec);
	txt_write_in_file(pcp_log,".Valor impreso: ");
	logear_int(pcp_log,atoi(men_imp_valor->dato));
	txt_write_in_file(pcp_log,"\n");

	enviar_men_comun_destruir(soc, R_IMPRIMIR,NULL,0);
}

void imprimir_texto(int32_t soc, t_men_comun *men_imp_texto){
	t_men_comun *men_id_prog;

	men_id_prog = socket_recv_comun(soc);

	if (men_id_prog->tipo != ID_PROG)
		printf("ERROR: esperaba el id de un programa y recibi: %i\n",men_id_prog->tipo);

	t_cpu *aux_cpu = get_cpu(soc);

	socket_send_comun(aux_cpu->soc_prog, men_imp_texto);

	pthread_mutex_lock(&mutex_uso_cola_cpu);
	queue_push(cola_cpu,aux_cpu);
	pthread_mutex_unlock(&mutex_uso_cola_cpu);

	txt_write_in_file(pcp_log,"IMPRIMIR_TEXTO por cpu con socket n°");
	logear_int(pcp_log,soc);
	txt_write_in_file(pcp_log," al programa con id:");
	logear_int(pcp_log,aux_cpu->id_prog_exec);
	txt_write_in_file(pcp_log,"\n");

	enviar_men_comun_destruir(soc, R_IMPRIMIR,NULL,0);
}

void fin_ejecucion(int32_t tipo,int32_t socket_cpu){
	t_cpu *aux_cpu = get_cpu(socket_cpu);

	t_pcb_otros *aux_pcb_otros = get_pcb_otros_exec(aux_cpu->id_prog_exec);

	txt_write_in_file(pcp_log,"Termino la ejecucion del programa:");
	logear_int(pcp_log,aux_pcb_otros->pcb->id);
	txt_write_in_file(pcp_log,"\n");

	if(tipo==SEGMEN_FAULT){
		txt_write_in_file(pcp_log,"Error por segmentation fault del programa:");
		logear_int(pcp_log,aux_pcb_otros->pcb->id);
	}

	aux_pcb_otros->tipo_fin_ejecucion = tipo;
	pasar_pcb_exit(aux_pcb_otros);

	aux_cpu->id_prog_exec = 0;
	aux_cpu->soc_prog = 0;

	pthread_mutex_lock(&mutex_uso_cola_cpu);
	queue_push(cola_cpu,aux_cpu);
	sem_incre(&cant_cpu_libres);
	pthread_mutex_unlock(&mutex_uso_cola_cpu);
}

t_pcb_otros *get_pcb_otros_exec_sin_quitarlo(int32_t id_proc){

	t_pcb_otros *aux_pcb_otros;
	int32_t i;

	int32_t tam = queue_size(colas->cola_exec);

	for(i=0 ;i < tam ;i++){

		aux_pcb_otros = queue_pop(colas->cola_exec);

		if (id_proc == aux_pcb_otros->pcb->id){

			queue_push(colas->cola_exec, aux_pcb_otros);
			aux_pcb_otros = queue_peek(colas->cola_exec);
			return aux_pcb_otros;
		}
		queue_push(colas->cola_exec, aux_pcb_otros);
	}
	printf("ERROR EN get_pcb_otros_exec_sin_quitarlo\n");
	return NULL;
}

void pasar_pcbBlock_ready(int32_t id_pcb){
	int32_t i = 0;

	pthread_mutex_lock(&mutex_block);
	int32_t tamanio_cola_block = queue_size(colas->cola_block);

	for (i=0; i<tamanio_cola_block; i++){
		t_pcb_otros *pcb_aux;
		pcb_aux =queue_pop(colas->cola_block);

		if (pcb_aux->pcb->id==id_pcb){
			pthread_mutex_lock(&mutex_ready);
			queue_push(colas->cola_ready, pcb_aux);
			sem_incre(&cant_ready);
			pthread_mutex_unlock(&mutex_ready);
			pthread_mutex_unlock(&mutex_block);
			return;
		}else {
			queue_push(colas->cola_block,pcb_aux);
		}
	}
	printf("ERROR: No se ha encontrado el pcb del proc IDº%i en la cola block\n",id_pcb);
	pthread_mutex_unlock(&mutex_block);
}

void pasar_pcb_exit(t_pcb_otros *pcb){
	pthread_mutex_lock(&mutex_exit);
	queue_push(colas->cola_exit,pcb);
	sem_incre(&cant_exit);
	pthread_mutex_unlock(&mutex_exit);
	sem_incre(&cant_multiprog);
}
/*
//Para buscar un pcb por su socket, xq no se sabe en que cola esta(cuando se cae la conexion), lo encuentra y lo pasa a exit
void mover_pcb_exit(int32_t soc_prog){

	t_pcb_otros *aux;

		pthread_mutex_lock(&mutex_new);
		aux = buscar_pcb(colas->cola_new, soc_prog);
		pthread_mutex_unlock(&mutex_new);

		if (aux!=NULL){
			pasar_pcb_exit(aux);
			return;
		}

		pthread_mutex_lock(&mutex_ready);
		aux = buscar_pcb(colas->cola_ready, soc_prog);
		pthread_mutex_unlock(&mutex_ready);

		if (aux!=NULL){
			pasar_pcb_exit(aux);
			return;
		}
		pthread_mutex_lock(&mutex_block);
		aux = buscar_pcb(colas->cola_block, soc_prog);
		pthread_mutex_lock(&mutex_block);

		if (aux!=NULL){
			pasar_pcb_exit(aux);
			return;
		}

		pthread_mutex_lock(&mutex_exec);
		aux = buscar_pcb(colas->cola_exec, soc_prog);
		pthread_mutex_lock(&mutex_exec);

		if (aux!=NULL){
			pasar_pcb_exit(aux);
			return;
		}

		//printf("ERROR mover_pcb_exit(): no se encontro el pcb con el socket nº:%i\n",soc_prog);
}

t_pcb_otros *buscar_pcb(t_queue *cola, int32_t soc_prog){
	int32_t i;
	t_pcb_otros *ret;

	for(i=0; i < queue_size(cola) ;i++){
		ret = queue_pop(cola);

		if (ret->n_socket == soc_prog)
			return ret;

		queue_push(cola, ret);
	}
	return NULL;
}*/

t_pcb_otros *actualizar_pcb_y_bloq(t_cpu *cpu){
	t_pcb_otros *aux_pcb_otros=get_pcb_otros_exec(cpu->id_prog_exec);

	t_pcb *pcb_recibido = socket_recv_quantum_pcb(cpu->soc_cpu)->pcb;
	actualizar_pcb(aux_pcb_otros->pcb,pcb_recibido);

	cpu->id_prog_exec = 0;
	cpu->soc_prog = 0;

	pthread_mutex_lock(&mutex_uso_cola_cpu);
	pthread_mutex_lock(&mutex_block);

	queue_push(cola_cpu,cpu);
	queue_push(colas->cola_block,aux_pcb_otros);

	sem_incre(&cant_cpu_libres);
	pthread_mutex_unlock(&mutex_uso_cola_cpu);
	pthread_mutex_unlock(&mutex_block);

	return aux_pcb_otros;
}

t_cpu *get_cpu_soc_prog(int32_t soc_prog){
	int32_t i;
	t_cpu *cpu;
	pthread_mutex_lock(&mutex_uso_cola_cpu);
	int32_t tam = queue_size(cola_cpu);

	for(i=0 ;i < tam ;i++){
		cpu = queue_pop(cola_cpu);

		if (cpu->soc_prog == soc_prog){
			pthread_mutex_unlock(&mutex_uso_cola_cpu);
			return cpu;
		}
		queue_push(cola_cpu, cpu);
	}
	pthread_mutex_unlock(&mutex_uso_cola_cpu);

	return NULL;
}

t_cpu *get_cpu(int32_t soc_cpu){

	int32_t i;
	t_cpu *cpu;
	pthread_mutex_lock(&mutex_uso_cola_cpu);
	int32_t tam = queue_size(cola_cpu);

	for(i=0 ;i < tam ;i++){
		cpu = queue_pop(cola_cpu);

		if (cpu->soc_cpu == soc_cpu){
			pthread_mutex_unlock(&mutex_uso_cola_cpu);
			return cpu;
		}
		queue_push(cola_cpu, cpu);
	}
	pthread_mutex_unlock(&mutex_uso_cola_cpu);
	printf("ERROR EN GET_CPU\n");
	return NULL;
}

t_pcb_otros *get_pcb_otros_exec(int32_t id_proc){

	t_pcb_otros *aux_pcb_otros;
	int32_t i;

	pthread_mutex_lock(&mutex_exec);
	int32_t tam = queue_size(colas->cola_exec);

	for(i=0 ;i < tam ;i++){
		aux_pcb_otros = queue_pop(colas->cola_exec);

		if (id_proc == aux_pcb_otros->pcb->id){
			pthread_mutex_unlock(&mutex_exec);
			return aux_pcb_otros;
		}
		queue_push(colas->cola_exec, aux_pcb_otros);
	}
	pthread_mutex_unlock(&mutex_exec);
	printf("ERROR en get_pcb_otros_exec\n");
	return NULL;
}

void *manejador_exit(){

	t_pcb_otros *aux_pcb_otros;

	while(quit_sistema){

		sem_decre(&cant_exit);
		pthread_mutex_lock(&mutex_exit);

		aux_pcb_otros = queue_pop(colas->cola_exit);

		pthread_mutex_unlock(&mutex_exit);

		enviar_men_comun_destruir(aux_pcb_otros->n_socket, aux_pcb_otros->tipo_fin_ejecucion,NULL,0);
		umv_destrui_pcb(aux_pcb_otros->pcb->id);
		free(aux_pcb_otros->pcb);
		free(aux_pcb_otros);
	}
	return NULL;
}

void umv_destrui_pcb(int32_t id_pcb){
	enviar_umv_mem_seg_destruir(soc_umv, DESTR_SEGS, id_pcb, 0);
	//recibo respuesta para sincronizar
	t_men_comun *men_resp = socket_recv_comun(soc_umv);
	if (men_resp->tipo != SINCRO_OK)
		printf("ERROR se esperaba, de la UMV, un tipo de mensaje SINCRO_OK y se recibio:%i\n",men_resp->tipo);
	printf("Segmentos del programa:%i destruidos\n", id_pcb);
	destruir_men_comun(men_resp);
}

void *manejador_new_ready(){

	while(quit_sistema){
		sem_decre(&cant_new);// Se fija que haya algun programa en la cola de New
		sem_decre(&cant_multiprog); // Se fija que no se supere el grado de multiprogramación

		pthread_mutex_lock(&mutex_new);
		pthread_mutex_lock(&mutex_ready);

		queue_push(colas->cola_ready, get_peso_min());

		pthread_mutex_unlock(&mutex_new);
		pthread_mutex_unlock(&mutex_ready);

		sem_incre(&cant_ready);
	}
	return NULL;
}

t_pcb_otros *get_peso_min(){
	int i, peso_min = 1000000;
	t_pcb_otros *aux;
	int tam_cola = queue_size(colas->cola_new);

	for (i=0;i < tam_cola;i++){
		aux = queue_pop(colas->cola_new);
		if(peso_min > aux->peso)
			peso_min = aux->peso;
		queue_push(colas->cola_new,aux);
	}
	for (i=0;i < tam_cola;i++){
		aux = queue_pop(colas->cola_new);
		if (peso_min == aux->peso)
			return aux;
		queue_push(colas->cola_new,aux);
	}
printf("ERROR: get_peso_min()\n");
return NULL;
}



void *manejador_ready_exec(){
	t_pcb_otros *aux_pcb_otros;
	t_cpu *cpu;

	while(quit_sistema){

		sem_decre(&cant_ready);
		sem_decre(&cant_cpu_libres);

		pthread_mutex_lock(&mutex_ready);
		pthread_mutex_lock(&mutex_uso_cola_cpu);

		cpu = get_cpu_libre();

		aux_pcb_otros = queue_pop(colas->cola_ready);

		if(aux_pcb_otros == NULL)
			printf("ERROR manejador_ready_exec()\n");

		enviar_cpu_pcb_destruir(cpu->soc_cpu,aux_pcb_otros->pcb,_QUANTUM_MAX);

		cpu->id_prog_exec = aux_pcb_otros->pcb->id;
		cpu->soc_prog = aux_pcb_otros->n_socket;

		queue_push(cola_cpu, cpu);
		queue_push(colas->cola_exec, aux_pcb_otros);

		pthread_mutex_unlock(&mutex_ready);
		pthread_mutex_unlock(&mutex_uso_cola_cpu);
	}
	return NULL;
}

t_cpu *get_cpu_libre(){

	int32_t i;
	t_cpu *cpu;

	int32_t tam = queue_size(cola_cpu);

	for(i=0 ;i < tam ;i++){
		cpu = queue_pop(cola_cpu);

		if (cpu->id_prog_exec == 0)
			return cpu;

		queue_push(cola_cpu, cpu);
	}
	printf("	No hay cpu libre\n");
	return NULL;
}

void recibir_resp_escrbir_seg(){//uso esto para q espere y no ponga en la cola de new un proceso con los segmentos vacios
	t_men_comun *men = socket_recv_comun(soc_umv);
	if(men->tipo != MEN_ALM_OK)
		printf("ERROR se esperaba un tipo de dato MEN_ALM_OK y se recibio:%i",men->tipo);
	destruir_men_comun(men);
}

/*t_pcb *crear_pcb_escribir_seg_UMV(t_men_comun *men_cod_prog ,t_resp_sol_mem *resp_sol ,int32_t contador_id_programa){
	t_metadata_program *metadata_program = metadata_desde_literal(men_cod_prog->dato);

	// Escribe el segmento de codigo
	enviar_umv_mem_seg_destruir(soc_umv , ESCRIBIR_SEG, contador_id_programa, 0);
	enviar_men_comun_destruir(soc_umv , CODIGO_SCRIPT,men_cod_prog->dato,men_cod_prog->tam_dato);
	recibir_resp_escrbir_seg();

	// Escribe el segmento de indice de etiquetas
	enviar_umv_mem_seg_destruir(soc_umv , ESCRIBIR_SEG, contador_id_programa, 0);
	char *etis = metadata_program->etiquetas;
	enviar_men_comun_destruir(soc_umv ,IND_ETI_FUNC,etis,metadata_program->etiquetas_size);
	recibir_resp_escrbir_seg();

	// Escribe el segmento de indice de codigo
	enviar_umv_mem_seg_destruir(soc_umv , ESCRIBIR_SEG, contador_id_programa, 0);
	int32_t tam_ind_cod = metadata_program->instrucciones_size*8;
	enviar_men_comun_destruir(soc_umv ,IND_COD,(void *)metadata_program->instrucciones_serializado,tam_ind_cod);
	recibir_resp_escrbir_seg();

	t_pcb *pcb = malloc(sizeof(t_pcb));
	pcb->id = contador_id_programa;
	pcb->dir_seg_codigo = resp_sol->dir_seg_codigo;
	pcb->dir_seg_stack = resp_sol->dir_seg_stack;
	pcb->dir_cont_actual = resp_sol->dir_seg_stack;
	pcb->dir_indice_codigo = resp_sol->dir_indice_codigo;
	pcb->dir_indice_etiquetas = resp_sol->dir_indice_etiquetas;
	pcb->program_counter = metadata_program->instruccion_inicio;
	pcb->cant_var_cont_actual = 0;
	pcb->tam_indice_etiquetas = metadata_program->etiquetas_size;

	return pcb;
} VERSION CORTA */

t_pcb *crear_pcb_escribir_seg_UMV(t_men_comun *men_cod_prog ,t_resp_sol_mem *resp_sol ,int32_t contador_id_programa){

	t_metadata_program *metadata_program = metadata_desde_literal(men_cod_prog->dato);
	t_men_comun* men;
	t_men_seg *men_esc;
	char *etis = metadata_program->etiquetas;

	// Escribe el segmento de codigo
	men_esc = crear_men_seg(ESCRIBIR_SEG, contador_id_programa, 0);
	socket_send_seg(soc_umv,men_esc);
	destruir_men_seg(men_esc);
	men = crear_men_comun(CODIGO_SCRIPT,men_cod_prog->dato,men_cod_prog->tam_dato);

	socket_send_comun(soc_umv, men);
	destruir_men_comun(men);
	men = socket_recv_comun(soc_umv);//uso esto para q espere y no ponga en la cola de new un proceso con los segmentos vacios
	if(men->tipo != MEN_ALM_OK)
		printf("ERROR se esperaba un tipo de dato MEN_ALM_OK y se recibio:%i",men->tipo);
	destruir_men_comun(men);

	// Escribe el segmento de indice de etiquetas
	socket_send_seg(soc_umv,crear_men_seg(ESCRIBIR_SEG, contador_id_programa, 0));
	men = crear_men_comun(IND_ETI_FUNC,etis,metadata_program->etiquetas_size);
	socket_send_comun(soc_umv, men);

	men = socket_recv_comun(soc_umv);//uso esto para q espere y no ponga en la cola de new un proceso con los segmentos vacios
	if(men->tipo != MEN_ALM_OK)
		printf("ERROR se esperaba un tipo de dato MEN_ALM_OK y se recibio:%i",men->tipo);

	// Escribe el segmento de indice de codigo
	socket_send_seg(soc_umv,crear_men_seg(ESCRIBIR_SEG, contador_id_programa, 0));
	int32_t tam_ind_cod = metadata_program->instrucciones_size*8;
	men = crear_men_comun(IND_COD,(void *)metadata_program->instrucciones_serializado,tam_ind_cod);
	socket_send_comun(soc_umv, men);

	men = socket_recv_comun(soc_umv);//uso esto para q espere y no ponga en la cola de new un proceso con los segmentos vacios
	if(men->tipo != MEN_ALM_OK)
		printf("ERROR se esperaba un tipo de dato MEN_ALM_OK y se recibio:%i",men->tipo);

	t_pcb *pcb = malloc(sizeof(t_pcb));
	pcb->id = contador_id_programa;
	pcb->dir_seg_codigo = resp_sol->dir_seg_codigo;
	pcb->dir_seg_stack = resp_sol->dir_seg_stack;
	pcb->dir_cont_actual = resp_sol->dir_seg_stack;
	pcb->dir_indice_codigo = resp_sol->dir_indice_codigo;
	pcb->dir_indice_etiquetas = resp_sol->dir_indice_etiquetas;
	pcb->program_counter = metadata_program->instruccion_inicio;
	pcb->cant_var_cont_actual = 0;
	pcb->tam_indice_etiquetas = metadata_program->etiquetas_size;

	destruir_men_comun(men);
	return pcb;
}

int32_t recibir_umv_dir_mem(int32_t tipo_men){
	t_men_comun *resp_umv = socket_recv_comun(soc_umv);
	if (resp_umv->tipo == tipo_men){
		int32_t ret = atoi(resp_umv->dato);
		destruir_men_comun(resp_umv);
		return ret;
	}
	if (resp_umv->tipo == MEM_OVERLOAD){
		destruir_men_comun(resp_umv);
		return -2;
	}
	printf("ERROR: se esperaba %i y obtube un %i\n",tipo_men,resp_umv->tipo);
	return -1;
}

int32_t gestionar_resp_sol_mem(){
	int32_t ret = -1;
	t_men_comun *resp_men_cod = socket_recv_comun(soc_umv);
	if (resp_men_cod->tipo == MEM_OVERLOAD){
		destruir_men_comun(resp_men_cod);
		return ret;
	}
	if((resp_men_cod->tipo != RESP_MEM_SEG_COD)&&(resp_men_cod->tipo != RESP_MEM_IND_ETI)&&(resp_men_cod->tipo != RESP_MEM_IND_COD)&&(resp_men_cod->tipo != RESP_MEM_SEG_STACK))
		printf("ERROR: se esperaba una respuesta de pedido de memoria y obtube un %i\n",resp_men_cod->tipo);
	ret = atoi(resp_men_cod->dato);
	destruir_men_comun(resp_men_cod);
	return ret;
}

/*t_resp_sol_mem * solicitar_mem(t_men_comun *men_cod_prog, int32_t tam_stack, int32_t id_prog){
	t_resp_sol_mem *resp_sol = malloc(sizeof(t_resp_sol_mem));
	int32_t tam = 0, resp;

	//pido mem para el codigo del script
	enviar_umv_mem_seg_destruir(soc_umv , PED_MEM_SEG_COD,id_prog,men_cod_prog->tam_dato);
	resp = gestionar_resp_sol_mem();
	if(resp == -1)
		return NULL;
	resp_sol->dir_seg_codigo = resp;

	//pido mem para el indice de etiquetas y funciones
	t_metadata_program* metadata_program = metadata_desde_literal(men_cod_prog->dato);
	tam = (metadata_program->etiquetas_size);
	enviar_umv_mem_seg_destruir(soc_umv ,  PED_MEM_IND_ETI , id_prog, tam);
	resp = gestionar_resp_sol_mem();
	if(resp == -1)
		return NULL;
	resp_sol->dir_indice_etiquetas= resp;

	//pido mem para el indice de codigo
	tam = (metadata_program->instrucciones_size*8);
	enviar_umv_mem_seg_destruir(soc_umv ,  PED_MEM_IND_COD , id_prog, tam);
	resp = gestionar_resp_sol_mem();
	if(resp == -1)
		return NULL;
	resp_sol->dir_indice_codigo = resp;

	//pido mem para el stack
	enviar_umv_mem_seg_destruir(soc_umv , PED_MEM_SEG_STACK , id_prog, tam_stack);

	resp = gestionar_resp_sol_mem();
	if(resp == -1)
		return NULL;
	resp_sol->dir_seg_stack = resp;

	return resp_sol;
}VERSION CORTA*/

t_resp_sol_mem * solicitar_mem(t_men_comun *men_cod_prog, int32_t tam_stack, int32_t id_prog){

	t_resp_sol_mem *resp_sol = malloc(sizeof(t_resp_sol_mem));
	resp_sol->memoria_insuficiente = 0;
	t_men_comun *resp_mem;
	t_men_seg *ped_mem;
	int32_t tam = 0;

	//pido mem para el codigo del script
	ped_mem = crear_men_seg(PED_MEM_SEG_COD,id_prog,men_cod_prog->tam_dato);
	socket_send_seg(soc_umv, ped_mem);
	t_men_comun *resp_men_cod = socket_recv_comun(soc_umv);

	if (resp_men_cod->tipo == MEM_OVERLOAD){
		resp_sol->memoria_insuficiente = MEM_OVERLOAD;
		destruir_men_comun(resp_men_cod);
		destruir_men_seg(ped_mem);
		return resp_sol;
	}

	if( resp_men_cod->tipo != RESP_MEM_SEG_COD)
		printf("ERROR: se esperaba %i y obtube un %i\n",RESP_MEM_SEG_COD,resp_men_cod->tipo);

	t_dir_mem dir_mem_cod = atoi(resp_men_cod->dato);
	resp_sol->dir_seg_codigo = dir_mem_cod;

	//pido mem para el indice de etiquetas y funciones
	t_metadata_program* metadata_program = metadata_desde_literal(men_cod_prog->dato);
	tam = (metadata_program->etiquetas_size);
	ped_mem = crear_men_seg( PED_MEM_IND_ETI , id_prog, tam);
	socket_send_seg(soc_umv, ped_mem);
	resp_mem = socket_recv_comun(soc_umv);

	if (resp_mem->tipo == MEM_OVERLOAD){
		resp_sol->memoria_insuficiente = MEM_OVERLOAD;
		destruir_men_comun(resp_mem);
		destruir_men_seg(ped_mem);
		return resp_sol;
	}

	if( resp_mem->tipo != RESP_MEM_IND_ETI)
		printf("ERROR: se esperaba %i y obtube un %i\n",RESP_MEM_IND_ETI,resp_mem->tipo);

	resp_sol->dir_indice_etiquetas= atoi(resp_mem->dato);

	//pido mem para el indice de codigo
	tam = (metadata_program->instrucciones_size*8);
	ped_mem = crear_men_seg( PED_MEM_IND_COD , id_prog, tam);
	socket_send_seg(soc_umv, ped_mem);
	resp_mem = socket_recv_comun(soc_umv);

	if (resp_mem->tipo == MEM_OVERLOAD){
		resp_sol->memoria_insuficiente = MEM_OVERLOAD;
		destruir_men_comun(resp_mem);
		destruir_men_seg(ped_mem);
		return resp_sol;
	}

	if( resp_mem->tipo != RESP_MEM_IND_COD)
		printf("ERROR: se esperaba %i y obtube un %i\n",RESP_MEM_IND_COD,resp_mem->tipo);

	resp_sol->dir_indice_codigo = atoi(resp_mem->dato);

	//pido mem para el stack
	ped_mem = crear_men_seg( PED_MEM_SEG_STACK , id_prog, tam_stack);
	socket_send_seg(soc_umv, ped_mem);
	resp_mem = socket_recv_comun(soc_umv);

	if (resp_mem->tipo == MEM_OVERLOAD){
		resp_sol->memoria_insuficiente = MEM_OVERLOAD;
		destruir_men_comun(resp_mem);
		destruir_men_seg(ped_mem);
		return resp_sol;
	}

	if( resp_mem->tipo != RESP_MEM_SEG_STACK)
		printf("ERROR: se esperaba %i y obtube un %i\n",RESP_MEM_SEG_STACK,resp_mem->tipo);

	t_dir_mem dir_mem_stack = atoi(resp_mem->dato);
	resp_sol->dir_seg_stack = dir_mem_stack;
	destruir_men_comun(resp_mem);
	destruir_men_seg(ped_mem);
	return resp_sol;
}

int32_t calcular_peso(t_men_comun *men_cod_prog){
	t_metadata_program *metadata_program = metadata_desde_literal(men_cod_prog->dato);
	return (5 * (metadata_program->cantidad_de_etiquetas) + 3 * (metadata_program->cantidad_de_funciones) + (metadata_program->instrucciones_size));
}

void *manejador_IO(t_IO *io){
	int32_t i;
	t_IO_espera *proceso;

	while(quit_sistema){
		sem_decre(&(io->cont_cant_proc));

		proceso = queue_pop(io->procesos);

		for(i=0; i<proceso->unidades;i++)
			usleep(io->io_sleep*1000);

		pasar_pcbBlock_ready(proceso->id_prog);
	}
	return NULL;
}

void enviar_cpu_pcb_destruir(int32_t soc,t_pcb *pcb,int32_t quantum){
	t_men_quantum_pcb * men_pcb= crear_men_quantum_pcb(PCB_Y_QUANTUM,quantum,pcb);
	socket_send_quantum_pcb(soc,men_pcb);
	destruir_quantum_pcb(men_pcb);
}

void actualizar_pcb(t_pcb *pcb, t_pcb *pcb_actualizado){
	pcb->cant_var_cont_actual = pcb_actualizado->cant_var_cont_actual;
	pcb->dir_cont_actual = pcb_actualizado->dir_cont_actual;
	pcb->program_counter = pcb_actualizado->program_counter;
}

t_datos_config *levantar_config(){
	void _imp_disp(char *key, t_IO *io){
		printf("	ID:%s ,Sleep:%i\n",key,io->io_sleep);
	}
	void _imp_var_glob(char* key, int32_t valor){
		printf("	ID:%s ,Valor:%i\n", key, valor);
	}
	void _imp_sem(char* key, t_semaforo *un_sem){
		printf("	ID:%s ,Valor:%i\n", key, un_sem->valor);
	}
	int i;
	t_config *diccionario_config = config_create("./kernel/kernel_config");
	t_datos_config *ret = malloc(sizeof(t_datos_config));
	ret->puerto_prog = config_get_string_value( diccionario_config, "Puerto_prog");
	ret->puerto_cpu = config_get_string_value( diccionario_config, "Puerto_CPU");
	ret->ip_umv = config_get_string_value( diccionario_config, "IP_UMV");
	ret->puerto_umv = config_get_string_value( diccionario_config, "Puerto_umv");
	char **id_ios = config_get_array_value( diccionario_config, "ID_HIO");
	char **io_sleeps = config_get_array_value( diccionario_config, "HIO");


	for (i=0; id_ios[i] != '\0'; i++){

		t_IO *new_IO = malloc(sizeof(t_IO));
		crear_cont(&(new_IO->cont_cant_proc) , 0);
		new_IO->io_sleep = atoi(io_sleeps[i]);
		new_IO->procesos = queue_create();

		dictionary_put(dicc_disp_io, id_ios[i],new_IO);
		pthread_create(&(new_IO->hilo), NULL, (void *)manejador_IO, (void*)new_IO);
	}

	free(id_ios);
	free(io_sleeps);

	char **aux_id_sem = config_get_array_value( diccionario_config, "SEMAFOROS");
	char **aux_valor = config_get_array_value( diccionario_config, "VALOR_SEMAFORO");

	for(i=0;aux_id_sem[i] != '\0'; i++){
		t_semaforo *new_sem = malloc(sizeof(t_semaforo));
		new_sem->valor = atoi(aux_valor[i]);
		new_sem->procesos = queue_create();
		dictionary_put(dicc_sem, aux_id_sem[i],new_sem);
	}

	free(aux_id_sem);
	free(aux_valor);

	ret->multiprogramacion = config_get_int_value( diccionario_config, "MULTIPROGRAMACION");
	_QUANTUM_MAX = config_get_int_value( diccionario_config, "QUANTUM");
	_RETARDO = config_get_int_value( diccionario_config, "RETARDO");
	ret->tam_stack = config_get_int_value( diccionario_config, "TAMANIO_STACK");
	char **aux_var_glob = config_get_array_value( diccionario_config, "VARIABLES_GLOBALES");

	int32_t valor_ini = 0;
	for (i=0; aux_var_glob[i] != '\0'; i++)
		dictionary_put(dicc_var, aux_var_glob[i],valor_ini);
	free(aux_var_glob);

	printf("\n\n--------------------Archivo Config--------------------------------\n");
	printf("Puerto Proc-Prog = %s\n", ret->puerto_prog);
	printf("Puerto CPU = %s\n", ret->puerto_cpu);
	printf("UMV\n");
	printf("	IP     = %s\n", ret->ip_umv);
	printf("	Puerto = %s\n", ret->puerto_umv);
	printf("Entrada/Salida\n");

	dictionary_iterator(dicc_disp_io, (void *)_imp_disp);

	printf("\nSemaforos\n");

	dictionary_iterator(dicc_sem, (void *)_imp_sem);

	printf("Varias\n");
	printf("	Multiprogramacion = %i\n", ret->multiprogramacion);
	printf("	Quantum = %i\n", _QUANTUM_MAX);
	printf("	Retardo =  %i\n", _RETARDO);
	printf("	Tamanio Stack = %i\n", ret->tam_stack);
	printf("Variables Globales\n");
	dictionary_iterator(dicc_var, (void *)_imp_var_glob);
	printf("------------------------------------------------------------------\n\n");
	return ret;
}

void handshake_cpu(int32_t soc){
	t_men_comun *men_hs = socket_recv_comun(soc);

	if(men_hs->tipo != HS_CPU)
		printf("ERROR HANDSHAKE CPU = %i\n",men_hs->tipo);
	destruir_men_comun(men_hs);

	enviar_men_comun_destruir(soc,HS_KERNEL,NULL,0);
}

void handshake_prog(int32_t soc){
	t_men_comun *men_hs = socket_recv_comun(soc);

	if(men_hs->tipo != HS_PROG){
		printf("ERROR se esperaba HS_PROG y se recibio %i\n",men_hs->tipo);
		txt_write_in_file(plp_log,"Error en el handshake con el programa \n");
	}
	destruir_men_comun(men_hs);

	enviar_men_comun_destruir(soc,HS_KERNEL,NULL,0);
}

void handshake_umv(char *ip_umv, char *puerto_umv){
	//envio a la UMV
	soc_umv = socket_crear_client(puerto_umv,ip_umv);

	enviar_men_comun_destruir(soc_umv,HS_KERNEL,NULL,0);
	//espero coneccion de la UMV
	t_men_comun *men_hs = socket_recv_comun(soc_umv);

	if(men_hs->tipo != HS_UMV){
		printf("ERROR HANDSHAKE");
		txt_write_in_file(plp_log,"Error en el handshake con la UMV \n");
	}
	printf("UMV conectada\n");
	txt_write_in_file(plp_log,"Conectado a la UMV \n");
	destruir_men_comun(men_hs);
}

void imprimir_cola(t_queue *cola){
	int32_t i;
	t_pcb_otros *aux;

	if (queue_is_empty(cola)){
		printf("*************COLA VACIA********************");
	}else {
		for(i=0; i < queue_size(cola) ;i++){
			aux = queue_pop(cola);
			printf("%i/%i - ",aux->pcb->id,aux->peso);
			queue_push(cola, aux);
		}
	}
}

void *imp_colas (){
	char opcion;

	while(quit_sistema){

		do {
			scanf("%c", &opcion);
		} while (opcion != 'i');

		pthread_mutex_lock(&mutex_new);
		pthread_mutex_lock(&mutex_ready);
		pthread_mutex_lock(&mutex_block);
		pthread_mutex_lock(&mutex_exec);
		pthread_mutex_lock(&mutex_exit);

		printf("--COLA-NEW---------------------------------------------------------\n");
		printf("|PID/PESO= ");
		imprimir_cola(colas->cola_new);
		printf("\n");

		printf("--COLA-READY-------------------------------------------------------\n");
		printf("|PID/PESO= ");
		imprimir_cola(colas->cola_ready);
		printf("\n");

		printf("--COLA-BLOCK-------------------------------------------------------\n");
		printf("|PID/PESO= ");
		imprimir_cola(colas->cola_block);
		printf("\n");

		printf("--COLA-EXEC--------------------------------------------------------\n");
		printf("|PID/PESO= ");
		imprimir_cola(colas->cola_exec);
		printf("\n");

		printf("--COLA-EXIT--------------------------------------------------------\n");
		printf("|PID/PESO= ");
		imprimir_cola(colas->cola_exit);
		printf("\n");
		printf("-------------------------------------------------------------------\n");

		pthread_mutex_unlock(&mutex_new);
		pthread_mutex_unlock(&mutex_ready);
		pthread_mutex_unlock(&mutex_block);
		pthread_mutex_unlock(&mutex_exec);
		pthread_mutex_unlock(&mutex_exit);
	}
	return NULL;
}

void crear_cont(sem_t *sem ,int32_t val_ini){

	if(sem_init(sem, 0, val_ini)== -1){
		perror("No se puede crear el semáforo");
		exit(1);
	}
}

void sem_decre(sem_t *sem){

	if (sem_wait(sem) == -1){
		perror("ERROR sem decrementar");
		exit(1);
	}
}

void sem_incre(sem_t *sem){

	if (sem_post(sem) == -1){
		perror("ERROR sem decrementar");
		exit(1);
	}
}

t_param_plp *ini_pram_plp(t_datos_config *diccionario_config){

	t_param_plp *aux = malloc(sizeof(t_param_plp));

	aux->puerto_prog = diccionario_config->puerto_prog;
	aux->ip_umv = diccionario_config->ip_umv;
	aux->puerto_umv = diccionario_config->puerto_umv;
	aux->tam_stack = diccionario_config->tam_stack;

	return aux;
}

t_param_pcp *ini_pram_pcp(t_datos_config *diccionario_config){

	t_param_pcp *aux = malloc(sizeof(t_param_pcp));

	aux->puerto_cpu = diccionario_config->puerto_cpu;

	return aux;
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
