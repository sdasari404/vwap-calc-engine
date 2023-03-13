package trade

import (
	"strconv"
	"log"
	"context"
	"sync"
)

type Data struct {
	Size  float64
	PriceInCents int64
}

type Trade struct {
	ProductId string
	Stream chan Data
	window []Data
	totalVolume float64
	totalPriceInCents float64
}

func New(productId string) (t Trade){
	return Trade{
		ProductId : productId,
		Stream : make(chan Data),
		window : []Data{},
		totalVolume : 0,
		totalPriceInCents : 0,
	}
}

func (t *Trade) Add (rawSize, rawPrice string) {
	size, err := strconv.ParseFloat(rawSize, 64)
	if err != nil {
		log.Fatal("Error parsing size: ", err)
	}

	price, err := strconv.ParseFloat(rawPrice, 64)
	if err != nil {
		log.Fatal("Error parsing price: ", err)
	}

	t.Stream <- Data {
		size,
		int64(price*100),
	}
}

func (t *Trade) Poll (ctx context.Context, wg *sync.WaitGroup) {
	log.Printf("Polling %s", t.ProductId)

	defer wg.Done()

	for {
		select {
		case data := <-t.Stream:
			t.update(data)
			t.dispatch(ctx)
		case <-ctx.Done():
			log.Println("Stopping poller")
			return
		}
	}
}

func (t *Trade) update (data Data) {
	// Update totals
	t.totalVolume += data.Size
	t.totalPriceInCents += data.Size * float64(data.PriceInCents)

	// Slide window
	t.window = append(t.window, data)

	if len(t.window) > 200 {
		head := t.window[0]
		t.totalVolume -= head.Size
		t.totalPriceInCents -= head.Size * float64(head.PriceInCents)

		tempWindow := make([]Data, 200)
		copy(tempWindow, t.window[1:])
		t.window = nil
		t.window = tempWindow
	}
}

func (t *Trade) dispatch (ctx context.Context) {
	currentVWAP := t.totalPriceInCents / t.totalVolume / 100
	log.Printf("%s: %0.2f", t.ProductId, currentVWAP)
}