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
	"math/rand"
	"os"
	"sync"
	"time"

	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

type DatosRegistro struct {
	Term      int
	operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
//
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	currentTerm int
	votedFor    int
	log         []DatosRegistro
	logEntry    int // Se comienza en 0 y se va sumando cuando se añaden logs

	commitIndex int
	lastApplied int
	estado      string

	wg               sync.WaitGroup
	aceptanCandidato int

	appendEntry chan bool

	// Se reinician tras elección
	nextIndex  []int // Siguiente entrada de registro a enviar a cada servidor
	matchIndex []int // Indice de la mayor entrada de registro de los servidores que sabemos que está replicada en el líder

	// mirar figura 2 para descripción del estado que debe mantenre un nodo Raft
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
/*
 * Goroutine de servicio de llamadas RPC.
 * Recibe la IP:puerto en la que escuchará las peticiones RPC
 */

func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = -1

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
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
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}

	// Añadir codigo de inicialización
	nr.lastApplied = -1
	nr.votedFor = -1
	nr.commitIndex = -1
	nr.currentTerm = 0
	nr.estado = "Seguidor"
	nr.nextIndex = make([]int, len(nr.Nodos))
	nr.matchIndex = make([]int, len(nr.Nodos))
	nr.wg = sync.WaitGroup{}
	nr.logEntry = -1
	nr.appendEntry = make(chan bool)

	go func() { time.Sleep(10000 * time.Millisecond); nr.gestionLider() }()
	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
//
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int
	var esLider bool
	var idLider int = nr.IdLider

	// Vuestro codigo aqui

	yo = nr.Yo
	mandato = nr.currentTerm
	if nr.votedFor == nr.Yo {
		esLider = true
	} else {
		esLider = false
	}

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	indice := -1
	mandato := -1
	EsLider := false
	idLider := -1
	valorADevolver := ""

	// Vuestro codigo aqui

	return indice, mandato, EsLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
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

// Metodo para RPC PedirVoto
//
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	if nr.currentTerm > peticion.Term { // Si el mandato del que pide es menor no se sigue comprobando
		reply.VoteGranted = false
		reply.Term = nr.currentTerm
	} else { // Sino, se sigue comprobando.
		// No se ha votado, o se ha votado al que pide y la entrada del que pide está al menos tan
		// actualizado. !!!!!!!!!! Añadir que la última entrada local comprometida sea de un mandato menor que el actual
		if (nr.votedFor == 0 || nr.votedFor == peticion.CandidateId) && peticion.LastLogIndex >= nr.commitIndex {
			nr.estado = "Follower"
			nr.currentTerm = peticion.Term
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
			reply.Term = nr.currentTerm
		}
	}

	return nil
}

type ArgAppendEntries struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // Indice del registro inmediatamente anterior a el/los que viene/n ahora

	PrevLogTerm  int             // Mandato del registro inmediatamente anterior
	Entries      []DatosRegistro // Se podría enviar más de una entrada por eficiencia
	LeaderCommit int
}

type Results struct {
	Term    int  // Mandato actual, por si el líder se tiene que actualizar
	Success bool // Verdad si el seguidor tenia el mismo PrevLogIndex and PrevLogTerm
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries, results *Results) error {
	//nr.Logger.Println("AppendEntries: %+v", args)

	nr.Mux.Lock()
	defer nr.Mux.Unlock()

	if nr.estado == "Dead" {
		return nil
	}

	if args.Term < nr.currentTerm { // El mandato del seguidor es mayor que el del 'lider'.
		results.Term = nr.currentTerm
		nr.estado = "Follower"
		results.Success = false

	} else if nr.log[args.PrevLogIndex].Term != args.PrevLogTerm { // El seguidor no tiene el mismo mandato que el lider para esa entrada
		nr.currentTerm = args.Term
		results.Term = nr.currentTerm
		results.Success = false

	} else { // Mismo mandato y el seguidor tiene las mismas registros que el lider hasta cierto índice
		if len(args.Entries) != 0 { // Si hay datos que copiar del lider (nuevo dato o entradas desactualizadas)
			nr.logEntry = args.PrevLogIndex

			for _, dato := range args.Entries {
				nr.logEntry++
				nr.log[nr.logEntry] = dato
			}

			if args.LeaderCommit > nr.commitIndex && args.LeaderCommit > nr.logEntry { // Compara entradas comprometidas con el lider
				nr.commitIndex = nr.logEntry
			} else if args.LeaderCommit > nr.commitIndex && args.LeaderCommit <= nr.logEntry {
				nr.commitIndex = args.LeaderCommit
			}
		}
	}

	nr.appendEntry <- true // Para indicar que ha recibido latido

	return nil
}

// ----- Metodos/Funciones a utilizar como clientes
//
//

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
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
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
//
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	ok := false

	err := nr.Nodos[nodo-1].CallTimeout("NodoRaft.PedirVoto", &args, &reply, 100*time.Millisecond)

	if err == nil { // El nodo ha devuelto un resultado
		nr.Mux.Lock()
		defer nr.Mux.Unlock()
		if !reply.VoteGranted {
			if reply.Term > nr.currentTerm { // Actualizar el mandato actual si no es el suyo
				nr.currentTerm = reply.Term
				nr.estado = "Follower"
				nr.votedFor = -1
			}
		} else {
			ok = true
			nr.aceptanCandidato++
			nr.wg.Done() // Solo ok si le aceptan como lider

		}
	}
	return ok
}

func (nr *NodoRaft) enviarLatido(nodo int, args *ArgAppendEntries, reply *Results) {
	if nr.nextIndex[nodo] <= args.PrevLogIndex {
		args.Entries = nr.log[nr.nextIndex[nodo]:args.PrevLogIndex]
	}
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries", args, reply, 100*time.Millisecond)
	if err == nil { // El nodo ha devuelto un resultado del latido
		nr.Mux.Lock()
		defer nr.Mux.Unlock()
		if reply.Term > nr.currentTerm { // El lider está en un mandato antiguo, pasa a seguidor
			nr.estado = "Follower"
			nr.votedFor = -1
			nr.currentTerm = reply.Term
		} else if reply.Term == nr.currentTerm && reply.Success { // Sigue siendo lider y el nodo está acutalizado
			if reply.Term > nr.currentTerm { // Actualizar el mandato actual si no es el suyo
				nr.nextIndex[nodo] = nr.logEntry
			}
		} else { // Sigue siendo el lider pero el nodo no tiene entradas desactualizadas
			nr.nextIndex[nodo]--
		}
	}
}

func (nr *NodoRaft) igualarIndex() {
	for i := 0; i < len(nr.Nodos); i++ {
		if i != nr.Yo {
			nr.nextIndex[i] = nr.logEntry + 1
			nr.matchIndex[i] = 0
		}
	}
}
func (nr *NodoRaft) gestionLider() {

	minTimeout := 50 // En milisegundos, máxima frecuencia de los latidos
	maxTimeout := 150
	maxElectionTimeout := 3000
	tiempoElecciones := 3000 // Igual meter esto en el struct del Nodo

	for true {
		//generar un tiemout aleatorio
		randTiempo := rand.Intn(maxTimeout-minTimeout) + minTimeout
		nr.Mux.Lock()
		if nr.commitIndex > nr.lastApplied {
			// Aplicar log a la maquina de estado
			// nr.lastApplied++;
		}
		status := nr.estado
		nr.Mux.Unlock()

		switch status {
		case "Follower": //Si es seguidor, esperar timeout

			select {
			case <-nr.appendEntry:

			case <-time.After(time.Duration(randTiempo) * time.Millisecond):
				nr.estado = "Candidate"
				nr.currentTerm++
			}
		case "Candidate": // Si es candidadato, enviar peticiones
			electionTimer := rand.Intn(maxElectionTimeout-maxTimeout) + maxTimeout
			nr.Mux.Lock()
			nr.votedFor = nr.Yo
			nr.aceptanCandidato = 1

			args := ArgsPeticionVoto{}
			args.Term = nr.currentTerm
			args.CandidateId = nr.Yo
			args.LastLogIndex = nr.commitIndex
			if nr.commitIndex >= 0 {
				args.LastLogTerm = nr.log[nr.commitIndex].Term
			}
			nr.Mux.Unlock()              // !!!!!!!!!!!! No se si habría que posponer el unlock un poco más
			nr.wg.Add(len(nr.Nodos) - 1) //!!!!!!!!!!!!!!!!!!!!!!Solo esperar a la mayoría
			done := make(chan struct{})

			go func() { // Esperar a que acaben todas las gorutinas y cerrar canal (para el select-case)
				nr.wg.Wait()
				close(done)
			}()

			for i := 1; i <= len(nr.Nodos); i++ {

				if i == nr.Yo {
					continue
				}

				reply := RespuestaPeticionVoto{}
				go nr.enviarPeticionVoto(i, &args, &reply)
			}

			select {
			case <-done: // Elección acaba en tiempo
				if nr.estado != "Follower" { // No ha cambiado su estado a candidato
					if nr.aceptanCandidato >= (len(nr.Nodos)/2)+1 { // Ha obtenido mayoría siendo candidato
						nr.estado = "Leader"
						nr.igualarIndex()
						args := ArgAppendEntries{}
						nr.Mux.Lock()
						args.Term = nr.currentTerm
						args.LeaderId = nr.Yo
						args.PrevLogIndex = nr.logEntry
						args.PrevLogTerm = nr.log[nr.logEntry].Term
						args.LeaderCommit = nr.commitIndex
						nr.Mux.Unlock()
						for i := 1; i <= len(nr.Nodos); i++ {

							if i == nr.Yo {
								continue
							}

							reply := Results{}
							go nr.enviarLatido(i, &args, &reply)
						}
					} else { // Todavía es candidato pero no ha obtenido mayoría. Nueva eleccion
						tiempoElecciones += electionTimer
					}
				}
			case <-time.After(time.Duration(electionTimer) * time.Millisecond):
				tiempoElecciones += electionTimer
			case <-nr.appendEntry: // Mensaje de otro nodo lider

			}
		case "Leader": // Si es líder, enviar latido
			args := ArgAppendEntries{}
			nr.Mux.Lock()
			args.Term = nr.currentTerm
			args.LeaderId = nr.Yo
			args.PrevLogIndex = nr.logEntry
			args.PrevLogTerm = nr.log[nr.logEntry].Term
			args.LeaderCommit = nr.commitIndex
			nr.Mux.Unlock()

			for i := 1; i <= len(nr.Nodos); i++ {

				if i == nr.Yo {
					continue
				}

				reply := Results{}
				go nr.enviarLatido(i, &args, &reply)
			}
		default:
			nr.estado = "Follower" // Si no es de ninguno de estos tipos no se hace nada. Se puede volver a poner a seguidor
		}
	}
}
