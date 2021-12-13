package main

import(

	"sync"
	"log"
	"net/rpc"
	"time"
	"fmt"
)

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func sendRequest(endpoint string, clave string, valor string, wg *sync.WaitGroup){
    defer wg.Done()
	
	client, err := rpc.Dial("tcp", endpoint)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	args := TipoOperacion{}
	args.Clave = clave
	args.Valor = valor
	args.Operacion = "Escritura"

	reply := ResultadoRemoto{}

	err = client.Call("NodoRaft.SometerOperacionRaft", args, &reply)
	if err != nil {
		log.Fatal("raft error:", err)
	}

}

func main(){

	numIt := 3
	var wg *sync.WaitGroup = new(sync.WaitGroup)
	endpoint := "127.0.0.1:29001"
	claves := [3]string{"Primero", "Segundo", "Tercero"}
	valores := [3]string{"Uno", "Dos", "Tres"}
	wg.Add(numIt)

	for i:=0; i<numIt; i++{
		fmt.Println("Iteracion ",i)
		go sendRequest(endpoint, claves[i], valores[i], wg)
		time.Sleep(1000*time.Millisecond)
	}
	time.Sleep(1*time.Second)
	wg.Wait()
}