package postrack

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	Event string
	Conn  struct {
		dsn    string
		replcn *pgconn.PgConn
		cn     *pgxpool.Pool
		slot   string
		events []Event
		lsn    pglogrepl.LSN
	}
	Table struct {
		Schema    string
		Name      string
		Condition string
		Override  bool
	}
	ConnOption  func(*Conn)
	TableOption func(*Table)
	HandleFunc  func(pglogrepl.LSN, string, Event, map[string]string, map[string]string)
)

const (
	INSERT   Event = "INSERT"
	UPDATE   Event = "UPDATE"
	DELETE   Event = "DELETE"
	TRUNCATE Event = "TRUNCATE"
)

func WithSelector(fields ...string) TableOption {
	return func(t *Table) {
		t.Condition = fmt.Sprintf("(%s)", strings.Join(fields, ","))
	}
}

func WithCondition(query string) TableOption {
	return func(t *Table) {
		t.Condition = fmt.Sprintf("WHERE %s", query)
	}
}

func WithOverride() TableOption {
	return func(t *Table) {
		t.Override = true
	}
}

func CreatePublicationId(name string) string {
	return fmt.Sprintf("publication_%s", name)
}

func New(dsn string, opts ...ConnOption) *Conn {
	conn := new(Conn)
	conn.dsn = dsn
	for _, opt := range opts {
		opt(conn)
	}
	return conn
}

func NewTable(schema string, name string, opts ...TableOption) *Table {
	table := new(Table)
	table.Schema = schema
	table.Name = name
	for _, opt := range opts {
		opt(table)
	}
	return table
}

func (conn *Conn) connect(ctx context.Context) error {
	config, err := pgxpool.ParseConfig(conn.dsn)
	if err != nil {
		return err
	}
	config.MaxConns = 5
	config.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	conn.cn = pool
	replcn, err := pgconn.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database", config.ConnConfig.User, config.ConnConfig.Password, config.ConnConfig.Host, config.ConnConfig.Port, config.ConnConfig.Database))
	if err != nil {
		return err
	}
	conn.replcn = replcn
	go conn.keepAlive(ctx, 5)
	return nil
}

func (conn *Conn) keepAlive(ctx context.Context, intervalSeconds int) {
	for ctx.Err() == nil {
		err := pglogrepl.SendStandbyStatusUpdate(context.TODO(), conn.replcn, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: conn.lsn,
		})
		if err != nil {
			log.Println(err)
		}
		<-time.After(time.Second * time.Duration(intervalSeconds))
	}
}

func (conn *Conn) PublicationExists(ctx context.Context, publicationId string) (bool, error) {
	res, err := conn.cn.Query(ctx, `SELECT TRUE as "Exists" FROM pg_publication WHERE pubname = $1`, publicationId)
	if err != nil {
		return false, err
	}
	defer res.Close()
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

func (conn *Conn) PublicationTableExists(ctx context.Context, publicationId string, table *Table) (bool, error) {
	res, err := conn.cn.Query(ctx, `SELECT TRUE as "Exists" FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3;`, publicationId, table.Schema, table.Name)
	if err != nil {
		return false, err
	}
	defer res.Close()
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
	defer res.Close()
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

func (conn *Conn) SetPublication(ctx context.Context, table *Table) error {
	id := CreatePublicationId(conn.slot)
	exists, err := conn.PublicationExists(ctx, id)
	if err != nil {
		return err
	}
	if exists {
		return conn.AlterPublication(ctx, table, table.Override)
	}
	return conn.AddPublication(ctx, table)
}

func (conn *Conn) AddPublication(ctx context.Context, table *Table) error {
	id := CreatePublicationId(conn.slot)
	_events := make([]string, 0)
	for _, event := range conn.events {
		_events = append(_events, string(event))
	}
	_, err := conn.replcn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s %s WITH (publish = '%s');", id, fmt.Sprintf("%s.%s", table.Schema, table.Name), table.Condition, strings.Join(_events, ","))).ReadAll()
	if err != nil {
		return err
	}
	return nil
}

func (conn *Conn) AlterPublication(ctx context.Context, table *Table, noOverride bool) error {
	id := CreatePublicationId(conn.slot)
	exists, err := conn.PublicationTableExists(ctx, id, table)
	if err != nil {
		return err
	}
	if exists {
		if noOverride {
			return nil
		}
		_, err := conn.replcn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s", id, fmt.Sprintf("%s.%s", table.Schema, table.Name))).ReadAll()
		if err != nil {
			return err
		}
	}
	_, err = conn.replcn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", id, fmt.Sprintf("%s.%s", table.Schema, table.Name))).ReadAll()
	if err != nil {
		return err
	}
	if len(table.Condition) != 0 {
		_, err = conn.replcn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s %s", id, fmt.Sprintf("%s.%s", table.Schema, table.Name), table.Condition)).ReadAll()
		if err != nil {
			return err
		}
	}
	return nil
}

func (conn *Conn) DropPublication(ctx context.Context, table *Table) error {
	id := CreatePublicationId(conn.slot)
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
	return conn.AddPublication(ctx, table)
}

func (conn *Conn) SetSlot(ctx context.Context, slot string) error {
	exists, err := conn.SlotExists(ctx, slot)
	if err != nil {
		return err
	}
	if exists {
		conn.slot = slot
		return nil
	}
	return conn.AddSlot(ctx, slot)
}

func (conn *Conn) AddSlot(ctx context.Context, slot string) error {
	conn.slot = slot
	_, err := pglogrepl.CreateReplicationSlot(ctx, conn.replcn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
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

func (conn *Conn) Changes(ctx context.Context, lsn int64, handleFunc HandleFunc) error {
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		id := CreatePublicationId(conn.slot)
		err = pglogrepl.StartReplication(
			ctx,
			conn.replcn,
			conn.slot,
			pglogrepl.LSN(lsn+1),
			pglogrepl.StartReplicationOptions{
				PluginArgs: []string{
					"proto_version '1'",
					fmt.Sprintf("publication_names '%s'", id),
				},
			},
		)
		wg.Done()
		conn.handler(ctx, handleFunc)
	}()
	wg.Wait()
	return err
}

func (conn *Conn) SetEvents(event []Event) {
	conn.events = event
}

func (conn *Conn) SetLSN(lsn int64) {
	conn.lsn = pglogrepl.LSN(lsn)
}

func (conn *Conn) Bootstrap(ctx context.Context, slot string, tables []Table, events []Event, lsn int64, handleFunc HandleFunc) error {
	conn.SetEvents(events)
	conn.SetLSN(lsn)
	err := conn.connect(ctx)
	if err != nil {
		return err
	}
	err = conn.SetSlot(ctx, slot)
	if err != nil {
		return err
	}
	for _, table := range tables {
		err := conn.SetPublication(ctx, &table)
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
		conn.SetLSN(int64(lsn))
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
				if data.OldTuple != nil {
					for index, column := range columns[data.RelationID] {
						oldValue[column] = string(data.OldTuple.Columns[index].Data)
					}
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
