#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>
#include <time.h>

#include <commons/string.h>
#include <commons/config.h>

#include <commons/collections/dictionary.h>
#include <commons/collections/list.h>





#define MAX 50

void encabezado(long byte, char *modo);
int clasificarComando(char *comando);
void operacion(int proceso, int base, int offset, int tamanio);
void retardo(int milisegundos);
void algoritmo(char *modo);
void compactar();
void dump();
int asignarMemoria(int tamanio);
void recorrerTablaSegmentos();
void recorrerLista(char *clave, void *ptrLista);
void insertarNodosBarrera ();
int asignarMemoriaAleatoria(int tamanio);


void destruirSegmentos(char *id_Prog);

void eliminarElemento(void *elemento);


void *ptrMemoria;
void *tablaProgramas;
char modoOperacion;
void *listaAuxiliar;
void *ptrConfig;
int memEstaOk;

typedef struct t_tabMem {
	int memLogica;
	int longitud;
	int memFisica;
	} TabMen;

typedef struct t_auxiliar {
	int ptrInicio;
	int ptrFin;
	void *ptrATabla;
	} ListAuxiliar;


void completarListaAuxiliar(TabMen *nodo);
bool compararListaAuxiliar(ListAuxiliar* nodo1, ListAuxiliar* nodo2);
void controlarSegFault();


int main()
{
	char **arrayComando;
	int a;
	char entrada[100];
	extern void *ptrMemoria;

	srand(time(0));
	//crear configuracion y solicitar memoria
	ptrConfig = config_create("umv_config.txt");
	encabezado(config_get_int_value(ptrConfig,"tamanio"),config_get_string_value(ptrConfig,"modo"));
	ptrMemoria = malloc(config_get_int_value(ptrConfig,"tamanio"));
	tablaProgramas = dictionary_create();

	printf("UMV >> ");
	gets(entrada);
	arrayComando =  string_split(entrada," ");
	a = clasificarComando(arrayComando[0]);

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
	gets(entrada);
	arrayComando =  string_split(entrada," ");
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
	if (!strcmp(modo,"WorstFit") || !strcmp(modo,"FirstFit"))
		modoOperacion=modo[0];
	else
		printf("Modo incorrecto");
}

bool compararListaAuxiliar(ListAuxiliar *nodo1, ListAuxiliar *nodo2){
	return ((nodo1->ptrInicio) < (nodo2->ptrInicio));
}

void compactar()
{
	ListAuxiliar *p1 = malloc(sizeof(ListAuxiliar));
	ListAuxiliar *p2 = malloc(sizeof(ListAuxiliar));
	int x;
	TabMen *actualizar;
	void *aux1;
	void *aux2;
	/*recorrer tabla de programas y crear una lista auxiliar ordenada, preguntar si hay espacio
	entre dos nodos, si hay se desplaza la memoria y se actualiza la tabla de programas*/
	recorrerTablaSegmentos();
	list_sort(listaAuxiliar, (void*)compararListaAuxiliar);
	for (x=0;x<list_size(listaAuxiliar)-1;x++){
		p1 = list_get(listaAuxiliar,x);
		p2 = list_get(listaAuxiliar,x+1);

		if (p2->ptrInicio != (p1->ptrFin) +1)
		{
			int aux = (p1->ptrFin)+1;
			aux1 = (&ptrMemoria + aux);
			aux = p2->ptrInicio;
			aux2 = (&ptrMemoria + aux);
			memcpy(aux1, aux2,(p2->ptrFin - p2->ptrInicio));
			actualizar = p2->ptrATabla;
			actualizar->memFisica = p1->ptrFin+1;
			p2->ptrFin = p1->ptrFin+1 + (p2->ptrFin - p2->ptrInicio);
			p2->ptrInicio = p1->ptrFin+1;

		}
	}
	list_destroy(listaAuxiliar);
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


void crearSegmento(char *id_Prog, int tamanio)
{
	void *lista;
	TabMen *nodoTab = malloc(sizeof(TabMen));


	nodoTab->longitud = tamanio;
	nodoTab->memFisica = asignarMemoria(tamanio);

	if (dictionary_has_key(tablaProgramas, id_Prog))
		{
		lista = dictionary_get(tablaProgramas,id_Prog);
			do {
			memEstaOk = 1;
			nodoTab->memLogica = asignarMemoriaAleatoria(tamanio);
			//list_iterate(lista,controlarSegmFault(nodoTab->memLogica));
			}while (!memEstaOk);
		list_add(lista,nodoTab);
		}
	else
	{
		nodoTab->memLogica = asignarMemoriaAleatoria(tamanio);
		lista = list_create();
		list_add(lista,nodoTab);
		dictionary_put(tablaProgramas, id_Prog, lista);
	}

}
// revisar
void controlarSegFault(TabMen *nodo, int numMemoria){
	if (((nodo->memLogica) < numMemoria) && ((nodo->memLogica + nodo->longitud) > numMemoria)){
		memEstaOk = 0;
	}

}

int asignarMemoria(int tamanio)
{

	ListAuxiliar *p1 = malloc(sizeof(ListAuxiliar));
	ListAuxiliar *p2 = malloc(sizeof(ListAuxiliar));
	int x;
	int espacioDisponible;
	int hayEspacio;
	int seCompacto;
	struct peorEspacio {
		int espacio;
		int offset;
	};

	struct peorEspacio pE;

	seCompacto =0;
	hayEspacio = 0;

	recorrerTablaSegmentos();
	list_sort(listaAuxiliar, (void *)compararListaAuxiliar);

	etiqueta_1:
	for (x=0;x<list_size(listaAuxiliar)-1;x++){
		p1 = list_get(listaAuxiliar,x);
		p2 = list_get(listaAuxiliar,x+1);

		if (p2->ptrInicio != (p1->ptrFin) +1){
			espacioDisponible = (p2->ptrInicio - p1->ptrFin -1);
			if (espacioDisponible >= tamanio)
			{
				hayEspacio=1;
				switch (modoOperacion){
					case'F':
						{
							return (p1->ptrFin+1);
							break;
						}
					case 'W':
						{
						if (espacioDisponible > pE.espacio){
							pE.espacio = espacioDisponible;
							pE.offset = p1->ptrFin+1;
							}
						break;
						}
				}
			}
				if (hayEspacio) return pE.offset;
				if (!hayEspacio && !seCompacto){
					seCompacto = 1;
					compactar();
					goto etiqueta_1;
				}
				if (!hayEspacio && seCompacto) return -1;
		}
	}
	return 0;
}

int asignarMemoriaAleatoria(int tamanio){
	int numero;
	numero = (rand());
	return numero;
}



void recorrerTablaSegmentos()
{
	listaAuxiliar= list_create();
	dictionary_iterator(tablaProgramas,recorrerLista);
	insertarNodosBarrera();
}

void recorrerLista(char* clave, void *ptrLista){
	list_iterate(ptrLista,(void *)completarListaAuxiliar);
}

void completarListaAuxiliar(TabMen *nodo)
{
	ListAuxiliar *nodoAux = malloc(sizeof(ListAuxiliar));

	nodoAux->ptrInicio = nodo->memFisica;
	nodoAux->ptrFin = nodo->memFisica + nodo->longitud;
	nodoAux->ptrATabla = nodo;

	list_add(listaAuxiliar, nodoAux);
}

void insertarNodosBarrera ()
{
	ListAuxiliar *nodoAux = malloc(sizeof(ListAuxiliar));
	nodoAux->ptrInicio = -1;
	nodoAux->ptrFin = -1;
	list_add_in_index(listaAuxiliar,0,nodoAux);
	nodoAux->ptrInicio=config_get_int_value(ptrConfig,"tamanio");
	nodoAux->ptrFin = config_get_int_value(ptrConfig,"tamanio");
	list_add(listaAuxiliar,nodoAux);
}

void destruirSegmentos(char *id_Prog){
	void *lista;
	lista = dictionary_get(tablaProgramas, id_Prog);
	list_destroy_and_destroy_elements(lista, (void *)eliminarElemento);
}
void eliminarElemento(void *elemento){
	free (elemento);
}
// error en iterator, en todas las funciones con orden superior
