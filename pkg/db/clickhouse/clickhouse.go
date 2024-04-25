package clickhouse

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Clickhouse struct {
	Connection driver.Conn
}

func Connect() (Clickhouse, error) {
	ch := Clickhouse{}
	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     []string{"localhost:9040"},
		Auth: clickhouse.Auth{
			Database: "default",
			//Username: "",
			//Password: "",
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "platon", Version: "Mk3"},
			},
		},

		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		//TLS: &tls.Config{
		//	InsecureSkipVerify: true,
		//},
	})

	if err != nil {
		return ch, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return ch, err
	}
	ch.Connection = conn
	return ch, nil
}
