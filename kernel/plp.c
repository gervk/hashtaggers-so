/*
 * plp.c
 *
 *  Created on: 13/05/2014
 *      Author: utnso
 */

#include <parser/metadata_program.h>
#include <commons/collections/list.h>
#include "planificadores.h"

#define BACKLOG 1 			//Es la cantidad de conexiones permitidas
#define IP "127.0.0.1"

int idProgr=1;

void *plp(){

	t_list *cola_new = list_create();
	extern t_list *cola_ready;

	extern int grado_multiprog;
	extern int grado_multiprog_max;

	extern int puerto_programas,puerto_umv;
	int socketKernel,socketPrograma,socketUmv;
	struct sockaddr_in programa;	//Estructuras en las que se guarda la info de las conexiones

	socketKernel = servidor(IP,puerto_programas);
	socketUmv = conectarse(IP,puerto_umv);

	listen(socketKernel,BACKLOG);
	socklen_t sin_size = sizeof(struct sockaddr_in);
	socketPrograma = accept(socketKernel, (struct sockaddr *)&programa, &sin_size);

	//Recepcion y deserializacion del script (quizas convenga hacerlo una funcion)
	int script_size;
	recv(socketPrograma , &script_size , sizeof(int) , 0);
	char *script = malloc(script_size);
	recv(socketPrograma , script , script_size , 0);

	t_metadata_program *metadata = metadata_desde_literal(script);
	estructura_pcb *pcb=crearPCB(metadata, script_size, socketPrograma);

	//Pedir segmentos a la umv
	if(pedirSegmentos(pcb,socketUmv)==0){
		avisarNoHayEspacio(pcb->socket_asociado);
		free(pcb);
		metadata_destruir(metadata);
	}
	else{
	//Envio el contenido de cada uno a la umv para que los guarde
		guardarContenido(pcb->segmento_codigo, pcb->tamanio_script, script, socketUmv);
		guardarContenido(pcb->indice_codigo, (pcb->cant_instrucciones)*8, metadata->instrucciones_serializado, socketUmv);
		guardarContenido(pcb->indice_etiquetas, pcb->tamanio_indice_etiquetas, script, socketUmv);
	}

	//Agrego el pcb a la cola de new:
	list_add(cola_new, pcb);
	list_sort(cola_new,(void*)comparar_peso);
	//Si el grado de multiprog me deja, lo paso de new a ready
	if(grado_multiprog < grado_multiprog_max){
		estructura_pcb *pcbAR = list_remove(cola_new,0);
		list_add(cola_ready, pcbAR);
		mostrar_ready(cola_ready);
	}

	close(socketKernel);
	list_destroy(cola_new);
	return 0;
}

estructura_pcb* crearPCB(t_metadata_program *metadata, int script_size, int socket){
	estructura_pcb *pcb=malloc(sizeof(estructura_pcb));

	pcb->id = idProgr++;
	pcb->program_counter = metadata->instruccion_inicio;
	pcb->tamanio_indice_etiquetas = metadata->etiquetas_size;
	pcb->cant_instrucciones = metadata->instrucciones_size;
	pcb->tamanio_script = script_size;
	pcb->peso = calcularPeso(metadata);
	pcb->socket_asociado = socket;

	return pcb;
}

int calcularPeso(t_metadata_program *metadata){
	return (5 * (metadata->cantidad_de_etiquetas) + 3 * (metadata->cantidad_de_funciones) + (metadata->instrucciones_size));
}

int pedirSegmentos(estructura_pcb *pcb, int socketUmv){
	extern int stack;

	pcb->segmento_codigo = pedirSegmento(pcb->id,pcb->tamanio_script,socketUmv);
	if(pcb->segmento_codigo==-1){
		return 0;
	}
	pcb->indice_etiquetas = pedirSegmento(pcb->id,pcb->tamanio_indice_etiquetas,socketUmv);
	if  ( pcb->indice_etiquetas==-1){
		pedirDestruirSegmentos(pcb->id,socketUmv);
		return 0;
	}
	pcb->indice_codigo = pedirSegmento(pcb->id,(pcb->cant_instrucciones)*8,socketUmv);
	if (pcb->indice_codigo==-1){
		pedirDestruirSegmentos(pcb->id,socketUmv);
		return(0);
	}
	pcb->segmento_stack=pedirSegmento(pcb->id, stack, socketUmv);
	if (pcb->segmento_stack==-1){
		pedirDestruirSegmentos(pcb->id,socketUmv);
		return(0);
	}
	return 1;
}

int pedirSegmento(int id_programa, int tamanio, int socket){
	char *pedido = malloc(sizeof(int) *3);
	int id_mensaje = 1;
	int direcc_segmento;

	memcpy(pedido,&id_mensaje,sizeof(id_mensaje));
	memcpy(pedido+sizeof(id_mensaje),&id_programa,sizeof(id_programa));
	memcpy(pedido+sizeof(id_mensaje)+sizeof(id_programa),&tamanio,sizeof(tamanio));
printf("id programa: %d \n",*(pedido+sizeof(id_mensaje)));
printf("tamaño pedido: %d \n",tamanio);
	send(socket,pedido,sizeof(int)*3,0);
	recv(socket,&direcc_segmento,sizeof(int),0);
printf("Direccion devuelta %d \n",direcc_segmento);
	return direcc_segmento;
}

int pedirDestruirSegmentos(int id_programa,int socket){
	char *pedido = malloc(sizeof(int)*2);
	int id_mensaje = 2;

	memcpy(pedido,&id_mensaje,sizeof(id_mensaje));
	memcpy(pedido+sizeof(id_mensaje),&id_programa,sizeof(id_programa));

	send(socket,pedido,sizeof(int)*2,0);
	return 0;
}

int guardarContenido(int direccion, int tamanio, void *contenido, int socketUmv){
	char *pedido = malloc(sizeof(int)*3+tamanio);
	int id_mensaje = 3;

	memcpy(pedido,&id_mensaje,sizeof(id_mensaje));
	memcpy(pedido+sizeof(id_mensaje),&tamanio,sizeof(int));
	memcpy(pedido+sizeof(id_mensaje)+sizeof(int),&direccion, sizeof(int));
	memcpy(pedido+sizeof(int)*3,contenido, tamanio);

	send(socketUmv,pedido,sizeof(int)*3+tamanio,0);
	return 0;
}

int avisarNoHayEspacio(int socket){
	int id_mensaje=1;
	send(socket,&id_mensaje,sizeof(int),0);
	return 0;
}

bool comparar_peso(estructura_pcb *pcb1,estructura_pcb *pcb2){
	return (pcb1->peso < pcb2->peso);
}

void imprimirIdPcb(estructura_pcb *pcb){
	printf("Id programa: %d \n",pcb->id);
}

void mostrar_ready(t_list *ready){
	list_iterate(ready, (void*)imprimirIdPcb);
}