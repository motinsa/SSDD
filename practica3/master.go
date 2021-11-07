/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: octubre de 2021
* FICHERO: worker.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes la practica 3
 */
package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"practica3/com"
	"sync"
	"time"
)

const (
	NORMAL   = iota // NORMAL == 0
	DELAY    = iota // DELAY == 1
	CRASH    = iota // CRASH == 2
	OMISSION = iota // IOTA == 3
)

type PrimesImpl struct {
	WorkerConf []com.Config
	TimeOut	int
	reqChan	chan com.TPInterval
	repChan chan []int
}

func isPrime(n int) (foundDivisor bool) {
	foundDivisor = false

	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

func (p *PrimesImpl) Stop(n int, result *int) error {
	os.Exit(n)
	return nil
}

// PRE: verdad
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func findPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.A; i <= interval.B; i++ {
		if isPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
// 		intervalo [interval.A, interval.B]
func (p *PrimesImpl) FindPrimes(interval com.TPInterval, primeList *[]int) error {

	return nil
}
func New() *PrimesImpl {
	h := &PrimesImpl{}
	err := rpc.Register(h)
	if err != nil {
		panic(err)
	}
	return h
}
func main() {
	if len(os.Args) == 2 {
		request := make(chan com.TPInterval)
		respuesta := make(chan []int)
		pi :=PrimesImpl{config,10000,request,respuesta}
		rpc.Register(&pi)
		rpc.HandleHTTP()
		l, e := net.Listen("tcp",os.Args[1]+":"+os.Args[2])
		if e != nil{
			log.Fatal("listen error:",e)
		}
		http.Serve(l, nil)
	}
	
}
