package main

import (
	"github.com/tsaikd/KDGoLib/sqlutil"
)

type selectDataType struct {
	ForumID string          `db:"forumID"`
	Data    selectForumType `db:"data"`
}

type selectForumType struct {
	ForumID string `db:"forumID"`
	Name    string `db:"name"`
	Lorem   string `db:"lorem"`
	Created string `db:"created"`
	Threads []struct {
		ForumID  string `db:"forumID"`
		ThreadID string `db:"threadID"`
		Name     string `db:"name"`
		Lorem    string `db:"lorem"`
		Created  string `db:"created"`
		Posts    []struct {
			ThreadID string `db:"threadID"`
			PostID   string `db:"postID"`
			Name     string `db:"name"`
			Lorem    string `db:"lorem"`
			Created  string `db:"created"`
		} `db:"posts"`
	} `db:"threads"`
}

// Scan decode SQL json value
func (t *selectForumType) Scan(value interface{}) (err error) {
	return sqlutil.SQLScanStrictJSON(t, value)
}
