package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type (
	Event string
	Conn  struct {
		host         string
		port         int
		user         string
		password     string
		database     string
		replcn       *pgconn.PgConn
		cn           *pgx.Conn
		publications map[string]bool
		slot         string
	}
	Table struct {
		Name      string
		Events    []Event
		Condition string
	}
	ConnOption func(*Conn)
	HandleFunc func(pglogrepl.LSN, string, Event, map[string]string, map[string]string)
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

func CreatePublicationId(name string) string {
	return fmt.Sprintf("publication_%s", name)
}

func New(host string, username string, password string, database string, opts ...ConnOption) *Conn {
	conn := new(Conn)
	conn.port = 5432
	conn.host = host
	conn.user = username
	conn.password = password
	conn.database = database
	conn.publications = make(map[string]bool)
	for _, opt := range opts {
		opt(conn)
	}
	return conn
}

func (conn *Conn) connect(ctx context.Context) error {
	replcn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", conn.user, conn.password, conn.host, conn.port, conn.database))
	if err != nil {
		return err
	}
	conn.replcn = replcn
	cn, err := pgx.Connect(ctx, fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", conn.host, conn.port, conn.user, conn.password, conn.database))
	if err != nil {
		return err
	}
	conn.cn = cn
	return nil
}

func (conn *Conn) PublicationExists(ctx context.Context, publicationId string) (bool, error) {
	res, err := conn.cn.Query(ctx, `SELECT TRUE as "Exists" FROM pg_publication WHERE pubname = $1`, publicationId)
	if err != nil {
		return false, err
	}
	if res.Next() {
		values, err := res.Values()
		if err != nil {
			return false, err
		}
		if len(values) == 0 {
			return false, fmt.Errorf("row contains no result")
		}
		result, ok := values[0].(bool)
		if !ok {
			return false, fmt.Errorf("expected boolean but found %T", values[0])
		}
		return result, nil
	}
	return false, nil
}

func (conn *Conn) SlotExists(ctx context.Context, publicationId string) (bool, error) {
	res, err := conn.cn.Query(ctx, `SELECT TRUE as "Exists" FROM pg_replication_slots WHERE slot_name = $1;`, publicationId)
	if err != nil {
		return false, err
	}
	if res.Next() {
		values, err := res.Values()
		if err != nil {
			return false, err
		}
		if len(values) == 0 {
			return false, fmt.Errorf("row contains no result")
		}
		result, ok := values[0].(bool)
		if !ok {
			return false, fmt.Errorf("expected boolean but found %T", values[0])
		}
		return result, nil
	}
	return false, nil
}

func (conn *Conn) AddPublication(ctx context.Context, table *Table) error {
	id := CreatePublicationId(table.Name)
	conn.publications[id] = true
	exists, err := conn.PublicationExists(ctx, id)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	_events := make([]string, 0)
	for _, event := range table.Events {
		_events = append(_events, string(event))
	}
	_, err = conn.replcn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s %s WITH (publish = '%s');", id, table.Name, table.Condition, strings.Join(_events, ","))).ReadAll()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Conn) DropPublication(ctx context.Context, table *Table) error {
	id := CreatePublicationId(table.Name)
	_, err := conn.replcn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", id)).ReadAll()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Conn) ReplacePublication(ctx context.Context, table *Table) error {
	err := conn.DropPublication(ctx, table)
	if err != nil {
		return err
	}
	id := CreatePublicationId(table.Name)
	_, err = conn.replcn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", id)).ReadAll()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Conn) AddSlot(ctx context.Context, slot string) error {
	conn.slot = slot
	exists, err := conn.SlotExists(ctx, slot)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn.replcn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		return err
	}
	return nil
}

func (conn *Conn) DropSlot(ctx context.Context, slot string) error {
	err := pglogrepl.DropReplicationSlot(ctx, conn.replcn, slot, pglogrepl.DropReplicationSlotOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (conn *Conn) Changes(ctx context.Context, lsn pglogrepl.LSN, handleFunc HandleFunc) error {
	publications := make([]string, 0)
	for key := range conn.publications {
		publications = append(publications, key)
	}
	err := pglogrepl.StartReplication(
		ctx,
		conn.replcn,
		conn.slot,
		lsn+1,
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
	go conn.handler(ctx, handleFunc)
	return nil
}

func (conn *Conn) Bootstrap(ctx context.Context, slot string, tables []Table, lsn pglogrepl.LSN, handleFunc HandleFunc) error {
	err := conn.AddSlot(ctx, slot)
	if err != nil {
		return err
	}
	for _, table := range tables {
		err := conn.AddPublication(ctx, &table)
		if err != nil {
			return err
		}
	}
	return conn.Changes(ctx, lsn, handleFunc)
}

func (conn *Conn) handler(ctx context.Context, handleFunc HandleFunc) {
	tables := make(map[uint32]string)
	columns := make(map[uint32][]string)
	for ctx.Err() == nil {
		msg, err := conn.replcn.ReceiveMessage(ctx)
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
				handleFunc(lsn, tables[data.RelationID], INSERT, newValue, nil)
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
				handleFunc(lsn, tables[data.RelationID], UPDATE, newValue, oldValue)
			}
		case *pglogrepl.DeleteMessage:
			{
				oldValue := make(map[string]string)
				for index, column := range columns[data.RelationID] {
					oldValue[column] = string(data.OldTuple.Columns[index].Data)
				}
				handleFunc(lsn, tables[data.RelationID], DELETE, nil, oldValue)
			}
		case *pglogrepl.TruncateMessage:
			{
				handleFunc(lsn, tables[data.RelationNum], TRUNCATE, nil, nil)
			}
		}
	}
}

func main() {
	conn := New("192.168.107.107", "root", "toor", "test")
	conn.Bootstrap(context.TODO(), "test_slot", []Table{{Name: "test", Events: []Event{INSERT, DELETE, UPDATE, TRUNCATE}}}, 0, func(l pglogrepl.LSN, s string, e Event, m1, m2 map[string]string) {
		fmt.Println(s, e, m1, m2)
	})

	fmt.Scanln()
}
