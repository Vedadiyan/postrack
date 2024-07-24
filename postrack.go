package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type (
	Event string
	Conn  struct {
		host     string
		port     int
		user     string
		password string
		database string
		cn       *pgconn.PgConn
	}

	ConnOption func(*Conn)
)

const (
	INSERT   Event = "INSERT"
	UPDATE   Event = "UPDATE"
	DELETE   Event = "DELETE"
	TRUNCATE Event = "TRUNCATE"
)

func WithPort(port int) ConnOption {
	return func(c *Conn) {
		c.port = port
	}
}

func New(host string, username string, password string, database string, opts ...ConnOption) *Conn {
	conn := new(Conn)
	conn.port = 5432
	conn.host = host
	conn.user = username
	conn.password = password
	conn.database = database
	for _, opt := range opts {
		opt(conn)
	}
	return conn
}

func (conn *Conn) connect(ctx context.Context) error {
	cn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", conn.user, conn.password, conn.host, conn.port, conn.database))
	if err != nil {
		return err
	}
	conn.cn = cn
	return nil
}

func (conn *Conn) configure(ctx context.Context, publicationId string, tables []string, events []Event) error {
	_events := make([]string, 0)
	for _, event := range events {
		_events = append(_events, string(event))
	}

	_, err := conn.cn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS publication_%s;", publicationId)).ReadAll()
	if err != nil {
		return err
	}
	_, err = conn.cn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION publication_%s FOR TABLE %s;", publicationId, strings.Join(tables, fmt.Sprintf(" WITH (publish = '%s'),", strings.Join(_events, ","))))).ReadAll()
	if err != nil {
		return err
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn.cn, publicationId, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		pgError, ok := err.(*pgconn.PgError)
		if !ok {
			return err
		}
		if pgError.Code != "42710" {
			return err
		}
	}
	return nil
}

func (conn *Conn) Listen(ctx context.Context, publicationId string, tables []string, events []Event, startLSN pglogrepl.LSN, cb func(pglogrepl.LSN, string, Event, map[string]string, map[string]string)) error {
	err := conn.connect(ctx)
	if err != nil {
		return err
	}
	err = conn.configure(ctx, publicationId, tables, events)
	if err != nil {
		return err
	}
	publications := make([]string, 0)
	for _, item := range tables {
		publications = append(publications, fmt.Sprintf("publication_%s", item))
	}
	err = pglogrepl.StartReplication(
		ctx,
		conn.cn,
		fmt.Sprintf("publication_%s_slot", conn.database),
		startLSN+1,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '2'",
				fmt.Sprintf("publication_names '%s'", strings.Join(publications, ",")),
			},
		},
	)
	if err != nil {
		return err
	}
	go func() {
		tables := make(map[uint32]string)
		columns := make(map[uint32][]string)
		for ctx.Err() == nil {
			msg, err := conn.cn.ReceiveMessage(ctx)
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				break
			}
			copyMsg, ok := msg.(*pgproto3.CopyData)
			if !ok {
				continue
			}
			if copyMsg.Data[0] != pglogrepl.XLogDataByteID {
				continue
			}
			walLog, err := pglogrepl.ParseXLogData(copyMsg.Data[1:])
			if err != nil {
				continue
			}
			data, err := pglogrepl.Parse(walLog.WALData)
			if err != nil {
				continue
			}
			lsn := walLog.WALStart

			switch data := data.(type) {
			case *pglogrepl.RelationMessage:
				{
					tables[data.RelationID] = data.RelationName
					columns[data.RelationID] = make([]string, 0)
					for _, column := range data.Columns {
						columns[data.RelationID] = append(columns[data.RelationID], column.Name)
					}
				}
			case *pglogrepl.InsertMessage:
				{
					newValue := make(map[string]string)
					for index, column := range columns[data.RelationID] {
						newValue[column] = string(data.Tuple.Columns[index].Data)
					}
					cb(lsn, tables[data.RelationID], INSERT, newValue, nil)
				}
			case *pglogrepl.UpdateMessage:
				{
					oldValue := make(map[string]string)
					for index, column := range columns[data.RelationID] {
						oldValue[column] = string(data.OldTuple.Columns[index].Data)
					}
					newValue := make(map[string]string)
					for index, column := range columns[data.RelationID] {
						newValue[column] = string(data.NewTuple.Columns[index].Data)
					}
					cb(lsn, tables[data.RelationID], UPDATE, newValue, oldValue)
				}
			case *pglogrepl.DeleteMessage:
				{
					oldValue := make(map[string]string)
					for index, column := range columns[data.RelationID] {
						oldValue[column] = string(data.OldTuple.Columns[index].Data)
					}
					cb(lsn, tables[data.RelationID], DELETE, nil, oldValue)
				}
			case *pglogrepl.TruncateMessage:
				{
					cb(lsn, tables[data.RelationNum], TRUNCATE, nil, nil)
				}
			}
		}
	}()
	return nil
}

func main() {
	conn := New("192.168.107.107", "root", "toor", "test")
	err := conn.Listen(context.TODO(), "test2", []string{"test"}, []Event{INSERT, UPDATE, DELETE, TRUNCATE}, 0, func(lsn pglogrepl.LSN, table string, event Event, newValue map[string]string, oldValue map[string]string) {
		fmt.Println(table, event, newValue, oldValue)
	})
	if err != nil {
		panic(err)
	}

	fmt.Scanln()
}
