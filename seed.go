package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"os"

	"github.com/drhodes/golorem"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
	pb "gopkg.in/cheggaaa/pb.v1"

	_ "github.com/lib/pq"
)

func main() {
	ctx := context.Background()

	dbMySQL, err := newMySQLConnection(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = dbMySQL.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	dbPGSQL, err := newPGSQLConnection()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err = dbPGSQL.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	err = insertData(ctx, dbMySQL, dbPGSQL, 100, 1000, 10)
	trace(err)
}

func trace(err error) {
	if err != nil {
		log.Println(err)
	}
}

func newMySQLConnection(ctx context.Context) (db *sqlx.DB, err error) {
	rootCertPool := x509.NewCertPool()
	pem, err := ioutil.ReadFile("server-ca.pem")
	if err != nil {
		return
	}

	rootCertPool.AppendCertsFromPEM(pem)

	clientCert := make([]tls.Certificate, 0, 1)
	certs, err := tls.LoadX509KeyPair("client-cert.pem", "client-key.pem")
	if err != nil {
		return
	}

	clientCert = append(clientCert, certs)
	mysql.RegisterTLSConfig("custom", &tls.Config{
		RootCAs:            rootCertPool,
		Certificates:       clientCert,
		InsecureSkipVerify: true,
	})

	if db, err = sqlx.Open("mysql", os.Getenv("MYSQL_URL")); err != nil {
		return
	}

	if err = db.Ping(); err != nil {
		return
	}

	db.SetMaxOpenConns(100)

	if _, err = db.ExecContext(ctx, `SET SESSION group_concat_max_len = 100000000;`); err != nil {
		return
	}

	return
}

func newPGSQLConnection() (db *sqlx.DB, err error) {
	if db, err = sqlx.Open("postgres", os.Getenv("PGSQL_URL")); err != nil {
		return
	}

	if err = db.Ping(); err != nil {
		return
	}

	db.SetMaxOpenConns(80)
	return
}

func insertData(
	ctx context.Context,
	txMySQL *sqlx.DB,
	txPGSQL *sqlx.DB,
	forumCount int,
	threadCountPerForum int,
	postCountPerThread int,
) (err error) {
	total := int64(forumCount + forumCount*threadCountPerForum + forumCount*threadCountPerForum*postCountPerThread)
	barMySQL := pb.New64(total).Start()
	defer barMySQL.Finish()
	barPGSQL := pb.New64(total).Start()
	defer barPGSQL.Finish()

	type chanType struct {
		ID    string
		Name  string
		Lorem string
	}

	forumChan := make(chan chanType, 10)
	threadChan := make(chan chanType, 200)
	postChan := make(chan chanType, 200)

	doneCtx, done := context.WithCancel(ctx)
	defer done()

	cleanChannel := func() {
		done()
		<-forumChan
		<-threadChan
		<-postChan
	}

	egProducer, ctxProducer := errgroup.WithContext(doneCtx)
	egProducer.Go(func() error {
		for {
			select {
			case <-ctxProducer.Done():
				return nil
			default:
				forumChan <- chanType{
					ID:    uuid.New().String(),
					Name:  lorem.Sentence(1, 3),
					Lorem: lorem.Sentence(3, 6),
				}
			}
		}
	})
	egProducer.Go(func() error {
		for {
			select {
			case <-ctxProducer.Done():
				return nil
			default:
				threadChan <- chanType{
					ID:    uuid.New().String(),
					Name:  lorem.Sentence(3, 10),
					Lorem: lorem.Sentence(50, 100),
				}
			}
		}
	})
	egProducer.Go(func() error {
		for {
			select {
			case <-ctxProducer.Done():
				return nil
			default:
				postChan <- chanType{
					ID:    uuid.New().String(),
					Name:  lorem.Sentence(3, 10),
					Lorem: lorem.Sentence(50, 200),
				}
			}
		}
	})

	type insertType struct {
		Type   int
		Forum  chanType
		Thread chanType
		Post   chanType
	}
	insertMySQLChan := make(chan insertType, 1000)
	insertPGSQLChan := make(chan insertType, 1000)

	egWorker, ctxWorker := errgroup.WithContext(ctx)

	egWorker.Go(func() error {
		for {
			select {
			case <-ctxWorker.Done():
				return nil
			case data, ok := <-insertMySQLChan:
				if !ok {
					return nil
				}
				barMySQL.Increment()
				switch data.Type {
				case 1:
					if err := insertForum(ctx, txMySQL, data.Forum.ID, data.Forum.Name, data.Forum.Lorem); err != nil {
						return err
					}
				case 2:
					if err := insertThread(ctx, txMySQL, data.Forum.ID, data.Thread.ID, data.Thread.Name, data.Thread.Lorem); err != nil {
						return err
					}
				case 3:
					if err := insertPost(ctx, txMySQL, data.Thread.ID, data.Post.ID, data.Post.Name, data.Post.Lorem); err != nil {
						return err
					}
				}
			}
		}
	})
	egWorker.Go(func() error {
		for {
			select {
			case <-ctxWorker.Done():
				return nil
			case data, ok := <-insertPGSQLChan:
				if !ok {
					return nil
				}
				barPGSQL.Increment()
				switch data.Type {
				case 1:
					if err := insertForum(ctx, txPGSQL, data.Forum.ID, data.Forum.Name, data.Forum.Lorem); err != nil {
						return err
					}
				case 2:
					if err := insertThread(ctx, txPGSQL, data.Forum.ID, data.Thread.ID, data.Thread.Name, data.Thread.Lorem); err != nil {
						return err
					}
				case 3:
					if err := insertPost(ctx, txPGSQL, data.Thread.ID, data.Post.ID, data.Post.Name, data.Post.Lorem); err != nil {
						return err
					}
				}
			}
		}
	})

	for fc := 0; fc < forumCount; fc++ {
		forumItem := <-forumChan
		insertMySQLChan <- insertType{
			Type:  1,
			Forum: forumItem,
		}
		insertPGSQLChan <- insertType{
			Type:  1,
			Forum: forumItem,
		}

		for tc := 0; tc < threadCountPerForum; tc++ {
			threadItem := <-threadChan
			insertMySQLChan <- insertType{
				Type:   2,
				Forum:  forumItem,
				Thread: threadItem,
			}
			insertPGSQLChan <- insertType{
				Type:   2,
				Forum:  forumItem,
				Thread: threadItem,
			}

			for pc := 0; pc < postCountPerThread; pc++ {
				postItem := <-postChan
				insertMySQLChan <- insertType{
					Type:   3,
					Forum:  forumItem,
					Thread: threadItem,
					Post:   postItem,
				}
				insertPGSQLChan <- insertType{
					Type:   3,
					Forum:  forumItem,
					Thread: threadItem,
					Post:   postItem,
				}
			}
		}
	}

	close(insertMySQLChan)
	close(insertPGSQLChan)
	if err = egWorker.Wait(); err != nil {
		return
	}

	cleanChannel()
	if err = egProducer.Wait(); err != nil {
		return
	}

	return
}

func insertForum(
	ctx context.Context,
	tx *sqlx.DB,
	forumID string,
	forumName string,
	forumLorem string,
) (err error) {
	_, err = tx.NamedExecContext(ctx, `
INSERT INTO forums
	(forumID, name, lorem)
VALUES
	(:forumID, :name, :lorem)
	;`, map[string]interface{}{
		"forumID": forumID,
		"name":    forumName,
		"lorem":   forumLorem,
	})
	if err != nil {
		panic(err)
	}
	return
}

func insertThread(
	ctx context.Context,
	tx *sqlx.DB,
	forumID string,
	threadID string,
	threadName string,
	threadLorem string,
) (err error) {
	_, err = tx.NamedExecContext(ctx, `
INSERT INTO threads
	(forumID, threadID, name, lorem)
VALUES
	(:forumID, :threadID, :name, :lorem)
	;`, map[string]interface{}{
		"forumID":  forumID,
		"threadID": threadID,
		"name":     threadName,
		"lorem":    threadLorem,
	})
	if err != nil {
		panic(err)
	}
	return
}

func insertPost(
	ctx context.Context,
	tx *sqlx.DB,
	threadID string,
	postID string,
	postName string,
	postLorem string,
) (err error) {
	_, err = tx.NamedExecContext(ctx, `
INSERT INTO posts
	(threadID, postID, name, lorem)
VALUES
	(:threadID, :postID, :name, :lorem)
	;`, map[string]interface{}{
		"threadID": threadID,
		"postID":   postID,
		"name":     postName,
		"lorem":    postLorem,
	})
	if err != nil {
		panic(err)
	}
	return
}
