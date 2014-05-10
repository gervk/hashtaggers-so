#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <commons/string.h>
#include <commons/config.h>
#include <commons/dictionary.h>
#include <commons/list.h>


#define MAX 50

void encabezado(long byte, char *modo);
int clasificarComando(char *comando);
void operacion(int proceso, int base, int offset, int tamanio);
void retardo(int milisegundos);
void algoritmo(char *modo);
void compactar();
void dump();

void *ptrMemoria;
void *tablaProgramas;
char modoOperacion;


int main()
{

	char **arrayComando;
	char entrada[100];
	int a;
	void *ptrConfig;

	extern void *ptrMemoria;

	ptrConfig = config_create("umv_config.txt");

	encabezado(config_get_int_value(ptrConfig,"tamanio"),config_get_string_value(ptrConfig,"modo"));

	ptrMemoria = malloc(config_get_int_value(ptrConfig,"tamanio"));

	tablaProgramas = dictionary_create();

	printf("UMV >> ");
	fgets(entrada, MAX, stdin);
	printf("%s",entrada);
	arrayComando =  string_get_string_as_array(entrada);
	a = clasificarComando(arrayComando[0]);
	printf("\n%s--%s\n",arrayComando[0],arrayComando[1]);

	while (a != 6)
	{
		switch (a){
			case 1:
				operacion(atoi(arrayComando[1]),atoi(arrayComando[2]),atoi(arrayComando[3]),atoi(arrayComando[4]));
				break;
			case 2:
				retardo(atoi(arrayComando[1]));
				break;
			case 3:
				algoritmo(arrayComando[1]);
				break;
			case 4:
				compactar();
				break;
			case 5:
				dump();
				break;
			default:
				printf("Comando desconocido");
				break;
			}

		printf("\nUMV >> ");
		fgets(entrada, MAX, stdin);
		arrayComando =  string_get_string_as_array(entrada);
		a = clasificarComando(arrayComando[0]);
	}
	return 0;
}


int clasificarComando (char *comando)
{
	int a;
	if (!strcmp(comando,"operacion")) a=1;
	if (!strcmp(comando,"retardo")) a=2;
	if (!strcmp(comando,"algoritmo")) a=3;
	if (!strcmp(comando,"compactacion")) a=4;
	if (!strcmp(comando,"dump")) a=5;
	if (!strcmp(comando,"exit"))
		{
		a=6;
		free(ptrMemoria);
		}
	printf("%d",a);
	return a;
}


void operacion(int proceso, int base, int offset, int tamanio)
{
	printf("operacion");
}
void retardo(int milisegundos)
{
	printf("retardo");
}

void algoritmo(char *modo)
{
	printf("algoritmo");
}
void compactar()
{
	printf("compactar");
}

void dump()
{
	printf("dump");
}


void encabezado(long byte, char *modo)
{
	printf("** UMV: Unidad de Memoria Virtual **\n");
	printf("Operando en modo: %s\n", modo);
	printf("Espacio reservado: %ld bytes\n", byte);
}

void crearSegmento(int id_Prog, int tamanio)
{
	void *lista;
	struct tabMem
	{
		int memLogica;
		int longitud;
		int memFisica;
	};
	struct tabMem nodoTab;

	nodoTab.memFisica = asignarMemoria(modoOperacion);
	nodoTab.longitud = tamanio;


	if dictionary_has_key(tablaProgramas, id_Prog)

	else

		lista = listCreate();



		list_add(lista,nodoTab);
		dictionary_put(tablaProgramas, id_Prog, lista);


}




/*
 * prueba.c
 *
 *  Created on: 09/05/2014
 *      Author: utnso
 */


#include<stdio.h>
#include<stdlib.h>
#include<commons/string.h>

int prueba(){
	 char* dato;
	 dato = malloc(10);
	 dato="1,2,3,4";
	 char **imprimir=string_get_string_as_array(dato);

	 printf("%s \n",dato);
	 printf("%s",imprimir[0]);
	 return 0;
}



