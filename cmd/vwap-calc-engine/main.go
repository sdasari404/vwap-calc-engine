package main

import (
	"cmay-vwap-calc-engine/feedsvc"
 	"cmay-vwap-calc-engine/trade"
	"os"
	"os/signal"
	"context"
	"sync"
	"log"
)

func main() {

	// create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// a WaitGroup for the goroutines to tell us they've stopped
	wg := sync.WaitGroup{}

	// TODO - make configurable
	host := "ws-feed.exchange.coinbase.com"
	channel := "matches"
	products := []string{"BTC-USD","ETH-USD","ETH-BTC"}

	for _, productId := range products {
		trade := trade.New(productId)
		feed := feedsvc.New(host)

		wg.Add(1)
		go feed.Subscribe(channel, trade, ctx, &wg)
		
		wg.Add(1)
		go trade.Poll(ctx, &wg)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt

	log.Println("Exiting program..")
	cancel()

	wg.Wait()
}
