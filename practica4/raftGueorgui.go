// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion


import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"math/rand"

	"raft/internal/comun/rpctimeout"
)

//  false deshabilita por completo los logs de depuracion
// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
const kEnableDebugLogs = true

// Poner a true para logear a stdout en lugar de a fichero
const kLogToStdout = true

// Cambiar esto para salida de logs en un directorio diferente
const kLogOutputDir = "./logs_raft/"


// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados 
type AplicaOperacion struct {
	indice int  // en la entrada de registro
	operacion interface{}
}

type DatosRegistro struct{
	Term 		int
	Clave		int
	Valor 		int
}


// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	mux   sync.Mutex       // Mutex para proteger acceso a estado compartido

	nodos []string // Conexiones RPC a todos los nodos (réplicas) Raft
	yo    int           // this peer's index into peers[]
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	logger *log.Logger

	currentTerm int
	votedFor    int
	log 		[]DatosRegistro

	comitIndex int
	lastApplied int
	estado string

	// Se reinician tras elección
	nextIndex []int // Siguiente entrada de registro a enviar a cada servidor
	matchIndex []int // Indice de la mayor entrada de reigstro de los servidores que sabemos que está replicada en el líder
}



// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []*rpc.Client, yo int, canalAplicar chan AplicaOperacion)
			*NodoRaft {
	nr := &NodoRaft{}
	nr.nodos = nodos
	nr.yo = yo

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].String()
		logPrefix := fmt.Sprintf("%s ", nombreNodo)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, nombreNodo,
								log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
			  kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		nr.logger.Println("logger initialized")
	} else {
		nr.logger = log.New(ioutil.Discard, "", 0)
	}

	// Your initialization code here (2A, 2B)
	nr.lastApplied = -1
	nr.votedFor = -1
	nr.comitIndex = -1
	nr.currentTerm = -1
	nr.estado = "Seguidor"
	nr.nextIndex = make([]int,len(nr.nodos))
	nr.matchIndex = make([]int,len(nr.nodos))

	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) Para() {

	// Vuestro codigo aqui

}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
func (nr *NodoRaft) ObtenerEstado() (int, int, bool) {
	var yo int
	var mandato int
	var esLider bool
	
	yo = nr.yo
	mandato = nr.currentTerm
	if nr.votedFor == nr.yo{
		esLider = true
	} else{
		esLider = false	
	}
	
	// Vuestro codigo aqui
	

	return yo, mandato, esLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver con
// rapidez
// 
// No hay garantia que esta operacion consiga comprometerse n una entrada 
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar 
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
func (nr *NodoRaft) SometerOperacion(operacion interface{}) (int, int, bool) {
	indice := nr.comitIndex+1
	mandato := nr.currentTerm
	EsLider := nr.votedFor == nr.yo
	

	// Vuestro codigo aqui
	

	return indice, mandato, EsLider
}


//
// ArgsPeticionVoto
// ===============
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
type ArgsPeticionVoto struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RespuestaPeticionVoto
// ================
//
// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
//
//
type RespuestaPeticionVoto struct {
	Term        int
	VoteGranted bool
}

//
// PedirVoto
// ===========
//
// Metodo para RPC PedirVoto
//
func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) {
	
	nr.mux.Lock()
	defer nr.mux.Unlock()

	if(nr.currentTerm > args.Term){ // Si el mandato del que pide es menor no se sigue comprobando
		reply.VoteGranted = false
		reply.Term =  nr.currentTerm
	}else{ // Sino, se sigue comprobando. 
		// No se ha votado, o se ha votado al que pide y la entrada del que pide está al menos tan
		// actualizado. !!!!!!!!!! Añadir que la última entrada local comprometida sea de un mandato menor que el actual
		if((nr.votedFor == 0 || nr.votedFor == args.CandidateId) && args.LastLogIndex >= nr.comitIndex ){
			reply.VoteGranted=true
		}
		else{
			reply.VoteGranted = false
			reply.Term = nr.currentTerm
		}


	}


}


// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumetnos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petiión perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *RequestVoteArgs,
												reply *RequestVoteReply) bool {
	
	// Esta función la invoca el nodo candidato, que para el nodo en la posicion int
	// llama a CallTiemout con el método PedirVoto, para que el seguidor ejecute esa
	// función

	CallTimeout(nr.nodos[nodo], "NodoRaft.PedirVoto", args, reply , 100*time.Millisecond) 											
	
	/*nr.Lock()
	defer nr.Unlock()
	ok := true

	if(reply.VoteGranted==false){ 
		if(reply.Term > nr.currentTerm){ // Actualizar el mandato actual si no es el suyo
			nr.currentTerm = reply.Term
			ok := bool
		}
		ok = false
	}*/
	


	return ok
}

/*func (nr *NodoRaft) gestionLider(){
	for true{
		//generar un tiemout aleatorio
		//nr.mux.Lock()
		switch nr.status {
		case 0: //Si es seguidor, esperar timeout
			randTiempo := rand.Intn()
			select
				case
				case <-time.After( * time.Second):
		case 1: // Si es candidadato, enviar peticiones

		case 2: // Si es líder, enviar latido
		
		default:
			
			return
		}
	}
}*/
