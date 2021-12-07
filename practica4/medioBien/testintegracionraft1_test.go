package testintegracionraft1

import (
	"raft/internal/comun/check"
	"fmt"
	//"log"
	//"crypto/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/despliegue"
	"raft/internal/raft"
	"raft/internal/comun/rpctimeout"
)

const (
	//hosts
	MAQUINA1      = "127.0.0.1"
	MAQUINA2      = "127.0.0.1"
	MAQUINA3      = "127.0.0.1"

	//puertos
	PUERTOREPLICA1 = "29011"
	PUERTOREPLICA2 = "29012"
	PUERTOREPLICA3 = "29013"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "/home/equipo/IngenieriaInformatica/QuintoSemestre/SistemasDistribuidos/practica4/Practica5/CodigoEsqueleto/raft/cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_ed25519"
)

// PATH de los ejecutables de modulo golang de servicio Raft
var PATH string = filepath.Join(os.Getenv("HOME"), "IngenieriaInformatica", "QuintoSemestre","SistemasDistribuidos", "practica4", "Practica5","CodigoEsqueleto","raft")

	// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; /usr/local/go/bin/go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
							3,
							[]string{REPLICA1, REPLICA2, REPLICA3},
							[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2 : No debería haber ningun primario, si SV no ha recibido aún latidos
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: tenemos el primer primario correcto
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Tres operaciones comprometidas en configuración estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
}


// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
							3,
							[]string{REPLICA1, REPLICA2, REPLICA3},
							[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T5:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T5:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}


// ---------------------------------------------------------------------
// 
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t *testing.T
	conectados []bool
	numReplicas int
	nodosRaft []rpctimeout.HostPort
	cr canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
										conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)
	
	return cfg
}

func (cfg *configDespliegue) stop() {
	cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

	cfg.cr.stop()
}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se pone en marcha una replica ?? - 3 NODOS RAFT
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t  // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas en remoto con un tiempo de espera incluido
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	fmt.Println("Esta parte hecha2")
	cfg.comprobarEstadoRemoto (0, 0, false, -1)
fmt.Println("Esta parte hecha3")
	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto (1, 0, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto (2, 0, false, -1)
	fmt.Println("Acaba de comprobar todo")
	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// Primer lider en marcha - 3 NODOS RAFT
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	// Se ha elegido lider ?
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)


	// Parar réplicas alamcenamiento en remoto
	cfg.stopDistributedProcesses()   // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	//t.Skip("SKIPPED FalloAnteriorElegirNuevoLiderTest3")

	fmt.Println(t.Name(), ".....................")

	cfg.startDistributedProcesses()

	fmt.Printf("Lider inicial\n")
	cfg.pruebaUnLider(3)


	// Desconectar lider
	// ???

	fmt.Printf("Comprobar nuevo lider\n")
	cfg.pruebaUnLider(3)
	

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()  //parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	t.Skip("SKIPPED tresOperacionesComprometidasEstable")

	// A completar ???
}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func(cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	t.Skip("SKIPPED AcuerdoApesarDeSeguidor")

	// A completar ???

	// Comprometer una entrada

	//  Obtener un lider y, a continuación desconectar una de los nodos Raft


	// Comprobar varios acuerdos con una réplica desconectada

	// reconectar nodo Raft previamente desconectado y comprobar varios acuerdos
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func(cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	t.Skip("SKIPPED SinAcuerdoPorFallos")

	// A completar ???

	// Comprometer una entrada

	//  Obtener un lider y, a continuación desconectar 2 de los nodos Raft


	// Comprobar varios acuerdos con 2 réplicas desconectada

	// reconectar lo2 nodos Raft  desconectados y probar varios acuerdos
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func(cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	t.Skip("SKIPPED SometerConcurrentementeOperaciones")

	// A completar ???

	// un bucle para estabilizar la ejecucion

	// Obtener un lider y, a continuación someter una operacion


	// Someter 5  operaciones concurrentes

	// Comprobar estados de nodos Raft, sobre todo
	// el avance del mandato en curso e indice de registro de cada uno
	// que debe ser identico entre ellos
}




// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				//fmt.Println("Comprobando ",i)
				if idNodo, mandato, eslider, idLider := cfg.obtenerEstadoRemoto(i);
																	  eslider {
					fmt.Println("idNodo ",idNodo," mandato ",mandato," esLider ",eslider," idLider ",idLider)
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		fmt.Println("Sale de la comprobacion ",iters)
		for mandato, lideres := range mapaLideres {
			fmt.Println("Mandato y lider ",mandato,lideres)
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
														mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {
			
			return mapaLideres[ultimoMandatoConLider][0]  // Termina
			
		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")
	
	return -1   // Termina
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
										indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
								raft.Vacio{}, &reply, 500 * time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")
	fmt.Println("La info obtenida es ",reply.IdNodo," ",reply.Mandato," ",reply.EsLider," ",reply.IdLider)
	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) startDistributedProcesses() {
 cfg.t.Log("Before starting following distributed processes: ", cfg.nodosRaft)

	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts( EXECREPLICACMD +
								" " + strconv.Itoa(i) + " " +
								rpctimeout.HostPortArrayToString(cfg.nodosRaft),
								[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)

		// dar tiempo para se establezcan las replicas
		time.Sleep(500 * time.Millisecond)
	}

	// aproximadamente 500 ms para cada arranque por ssh en portatil
	time.Sleep(1000 * time.Millisecond)
}

//
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio
	fmt.Println("Antes de para los procesos")
	for _, endPoint := range cfg.nodosRaft {
		err := endPoint.CallTimeout("NodoRaft.ParaNodo",
								raft.Vacio{}, &reply, 100 * time.Millisecond)
		check.CheckError(err, "Error en llamada RPC Para nodo")
	}
	fmt.Println("Se paran los procesos")
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
				 mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")
	fmt.Println("Esta parte hechae")
	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
						esLider != esLiderDeseado || idLider != IdLiderDeseado {
	  cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s con infos nodo: %d mandato: %d  y deseado: %d",
													idNodoDeseado, cfg.t.Name(), idNodo, mandato,mandatoDeseado)
	}

}
