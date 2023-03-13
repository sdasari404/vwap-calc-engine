package feedsvc

import (
	"cmay-vwap-calc-engine/trade"
	"github.com/gorilla/websocket"
	"net/url"
	"log"
	"github.com/gofrs/uuid"
	"context"
	"sync"
	"time"
)

type Feed interface {
	Subscribe(channel string, data trade.Trade, ctx context.Context, wg *sync.WaitGroup) 
}

type feed struct {
	conn *websocket.Conn
	numRetries int
	maxRetries int
	writeLock sync.Mutex
	readLock sync.Mutex
}

type payload struct {
	Action     string   `json:"type"`
	Channels   []string `json:"channels"`
	ProductIds []string `json:"product_ids"`
}

type match struct {
	Channel      string       `json:"type"`
	TradeId      int          `json:"trade_id"`
	SeqId        int          `json:"sequence"`
	MakerOrderId uuid.UUID    `json:"maker_order_id"`
	TakerOrderId uuid.UUID    `json:"taker_order_id"`
	Time         string       `json:"time"`
	ProductId    string       `json:"product_id"`
	Size         string       `json:"size"`
	Price        string       `json:"price"`
	Side         string       `json:"side"`
}

func New(host string) Feed{
	u := url.URL{Scheme: "wss", Host: host}
	log.Printf("connecting to %s", u.String())	

	feed := feed {
		numRetries : 0,
		maxRetries : 5,
	}

	feed.connect(u)

	return &feed
}

func (feed *feed) connect(u url.URL) {
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

	if err != nil {
		log.Println("Failed to connect: ", err)

		if feed.numRetries < feed.maxRetries {
			log.Println("Retrying after 5 seconds...")
			time.Sleep(5 * time.Second)
			feed.numRetries += 1
			feed.connect(u)
		} else {
			log.Fatal("Gave up after 5 retries")
		}

		return
	}

	feed.conn = c
}

func (feed *feed) Subscribe(channel string, data trade.Trade, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer feed.conn.Close()

	requestPayload := &payload {
		Action: "subscribe",
		Channels: []string{channel},
		ProductIds: []string{data.ProductId},
	 }

	 feed.writeToWS(requestPayload)

	 log.Printf("Subscribed to %s", data.ProductId)

	 done := make(chan struct{})
	 go feed.listen(data, done)

	 for {
		select {
			case <-done: return
			case <-ctx.Done():
					log.Println("Stopping feed.")
					close(data.Stream)
					feed.stop()
					log.Println("Successfully shut down feed.")
					return
		}
	 }
}

func (feed *feed) listen(data trade.Trade, done chan struct{}) {
	defer close(done)
	var lastSeqNo int

    for {
      message := match{}
	  feed.readLock.Lock()
      err := feed.conn.ReadJSON(&message)
	  feed.readLock.Unlock()
      if err != nil && !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
        log.Fatal("Failed to read message:", err)
		return
      } else {
			// Sequence numbers that are less than the previous number can be ignored
			if message.SeqId <= lastSeqNo {
				continue
			}

			log.Printf("seq: %d, product_id: %s, size: %s, price: %s", message.SeqId, message.ProductId, message.Size, message.Price)
			data.Add(message.Size, message.Price)
      }
    }
}

func (feed *feed) stop() {
	closeErr := feed.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if closeErr != nil {
	  log.Println("write close:", closeErr)
	  return
	}
}

func (feed *feed) writeToWS(requestPayload *payload) {
	feed.writeLock.Lock()
	defer feed.writeLock.Unlock()

	writeErr := feed.conn.WriteJSON(requestPayload)
	if writeErr != nil {
	   log.Panic("Failed to unsubscribe:", writeErr)
	}
}