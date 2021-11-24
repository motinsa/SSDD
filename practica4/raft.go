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

type ArgsAppend  struct{
	Term 			int
	LeaderId		int
	PrevLogIndex	int	// Indice del registro inmediatamente anterior a el/los que viene/n ahora

	PrevLogTerm		int // Mandato del registro inmediatamente anterior
	Entries 		[]DatosRegistro // Se podría enviar más de una entrada por eficiencia
	LeaderCommit	int // Última entrada comprometida del líder		
}

type RespuestaAppend struct{
	Term  			int // Mandato actual, por si el líder se tiene que actualizar
	Success 		bool // Verdad si el seguidor tenia el mismo PrevLogIndex and PrevLogTerm
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

	commitIndex int
	lastApplied int
	estado 		string

	wg  		sync.WaitGroup
	aceptanCandidato 	int

	appendEntry  	chan ArgsAppend

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
	nr.commitIndex = -1
	nr.currentTerm = -1
	nr.estado = "Seguidor"
	nr.nextIndex = make([]int,len(nr.nodos))
	nr.matchIndex = make([]int,len(nr.nodos))
	nr.wg = sync.WaitGroup{}

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
	indice := nr.commitIndex+1
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
func (nr *NodoRaft) PedirVoto(args *ArgsPeticionVoto, reply *RespuestaPeticionVoto) error{
	
	nr.mux.Lock()
	defer nr.mux.Unlock()
	if nr.estado = "Dead" {
		return nil
	}
	if (nr.currentTerm < args.Term) { // Si el mandato del que pide es menor entonces no puede ser lider por lo que se convierte en seguidor
		nr.estado = "Follower"
	} // Sino, se sigue comprobando. 
		// No se ha votado, o se ha votado al que pide y la entrada del que pide está al menos tan
		// actualizado. !!!!!!!!!! Añadir que la última entrada local comprometida sea de un mandato menor que el actual
		//esto no se muy bien que hacer
	if ((nr.votedFor == -1 || nr.votedFor == args.CandidateId) && args.LastLogIndex >= nr.commitIndex ){ //No sería nr.currentTerm = args.Term en vez de args.LastLogIndex >= nr.commitIndex
			reply.VoteGranted=true
			nr.votedFor = args.CandidateId


	}
	else{
			reply.VoteGranted = false
	
		}
	reply.Term = nr.currentTerm
	return nil
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
	
	err := CallTimeout(nr.nodos[nodo], "NodoRaft.PedirVoto", args, reply , 100*time.Millisecond) 											
	
	nr.Lock()
	defer nr.Unlock()
	ok := false

	if (err == nil){ // El nodo ha devuelto un resultado
		if (!reply.VoteGranted){ 
			if(reply.Term > nr.currentTerm){ // Actualizar el mandato actual si no es el suyo
				nr.currentTerm = reply.Term
				nr.estado = "Follower"
			}
		}
		else{
			ok = true
		}
	}

	return ok
}

func (nr *NodoRaft) gestionLider(){
	
	minTimeout := 50 // En milisegundos, máxima frecuencia de los latidos
	maxTimeout := 150
	maxElectionTimeout := 300
	tiempoElecciones := 0

	for true{
		//generar un tiemout aleatorio
		nr.mux.Lock()
		status := nr.estado
		nr.mux.Unlock()
		
		switch status {
		case "Follower": //Si es seguidor, esperar timeout
			randTiempo := rand.Intn(maxTimeout - minTimeout) + minTimeout
			select
				case
				case <-time.After(randTiempo * time.Millisecond):
					nr.estado = "Candidate"
					nr.currentTerm++
		case "Candidate": // Si es candidadato, enviar peticiones
			electionTimer = rand.Intn(maxElectionTimeout - maxTimeout) + maxTimeout
			nr.mux.Lock()
			nr.votedFor = nr.yo
			
			//var chanResp = make(chan bool)
			//go nr.HacerPeticiones(chanResp)
			votos := 1
			args := RequestVoteArgs{}
			args.Term = nr.currentTerm
			args.CandidateId = nr.yo
			args.LastLogIndex = nr.commitIndex
			args.LastLogTerm = nr.log[nr.commitIndex].Term
			
			nr.aceptanCandidato = 1
			nr.mux.Unlock() // !!!!!!!!!!!! No se si habría que posponer el unlock un poco más
			nr.wg.Add(len(nr.nodos)-1)
			done := make(chan struct{})

			go func(){ // Esperar a que acaben todas las gorutinas y cerrar canal (para el select-case)
				nr.wg.Wait()
				close(done)
			}()

			for i := 1; i <= len(nr.nodos); i++{

				if(i == nr.yo){ continue }

				reply := RequestVoteReply{}
				go nr.enviarPeticionVoto(i,args,reply)
			}

			select {
				case <-done:
					if(res){
						nr.estado = "Leader"
						// Enviar latido
					}

				case <- time.After(electionTimer * time.Millisecond):
					tiempoElecciones += electionTimer
				case newAppend <- nr.appendEntry: // Mensaje de otro nodo lider

			}
		case "Leader": // Si es líder, enviar latido
		
		default:
			// Si no es de ninguno de estos tipos no se hace nada. Se puede volver a poner a seguidor
			//nr.status = "Follower"
		}
	}
}

/*func (nr *NodoRaft) HacerPeticiones(chanResp chan bool){
	
	votos := 1
	args := RequestVoteArgs{}
	args.Term = nr.currentTerm
	args.CandidateId = nr.yo
	args.LastLogIndex = nr.commitIndex
	args.LastLogTerm = nr.log[nr.commitIndex].Term
	
	nr.aceptanCandidato = 0
	nr.wg.Add(len(nr.nodos)-1)
	done := make(chan struct{})
	go func(){ // Esperar a que acaben todas las gorutinas y cerrar canal (para el select-case)
		nr.wg.Wait()
		close(done)
	}()
	for i := 1; i <= len(nr.nodos); i++{

		if(i == nr.yo){ continue }

		reply := RequestVoteReply{}
		go nr.enviarPeticionVoto(i,args,reply)
	}

	// Si obtiene mayoría y sigue siendo candidato
	if((votos >= (len(nr.nodos) / 2) + 1) && nr.estado == "Candidate"){ 
		chanResp <- true
	}
	else{ // No ha obtenido mayoría o ya no es candidato
		chanResp <-false
	}

}*/
func (nr *NodoRaft) AppendEntries(args ArgsAppend,reply *RespuestaAppend ) error{
	nr.mux.Lock()
	defer nr.mux.Unlock()
	if nr.estado == "Dead"{
		return nil
	}
	nr.logger("AppendEntries: %+v", args)
	if args.Term > nr.currentTerm{
		nr.logger("Mandato menor en AppendEntries")
		nr.estado = "Follower"
	}
	reply.Success = false
	if args.Term == nr.currentTerm{
		if nr.estado != "Follower"{
			nr.estado = "Follower"
		}
		reply.Success = true
	}
	reply.Term = nr.currentTerm
	nr.logger("Respuesta Append: +%+v",*reply )
	return nil

}
