package beanstalk

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

const (
	cmdQuit               = "quit"
	cmdPut                = "put"
	cmdUse                = "use"
	cmdReserve            = "reserve"
	cmdReserveWithTimeout = "reserve-with-timeout"
	cmdReserveJob         = "reserve-job"
	cmdDelete             = "delete"
	cmdRelease            = "release"
	cmdBury               = "bury"
	cmdTouch              = "touch"
	cmdWatch              = "watch"
	cmdIgnore             = "ignore"
	endLine               = "\r\n"
	resInternalError      = "INTERNAL_ERROR" + endLine
	resUnknownCommand     = "UNKNOWN_COMMAND" + endLine
	resInserted           = "INSERTED %d" + endLine
	resBuriedID           = "BURIED %d" + endLine
	resUsing              = "USING %s" + endLine
	resReserved           = "RESERVED %d %d" + endLine + "%s" + endLine
	resTimedOut           = "TIMED_OUT" + endLine
	resDeadlineSoon       = "DEADLINE_SOON" + endLine
	resNotFound           = "NOT_FOUND" + endLine
	resDeleted            = "DELETED" + endLine
	resReleased           = "RELEASED" + endLine
	resBuried             = "BURIED" + endLine
	resTouched            = "TOUCHED" + endLine
	resWatching           = "WATCHING %d" + endLine
	resNotIgnored         = "NOT_IGNORED" + endLine
)

var MissingLineEnd = errors.New("expected crlf")

func readFullLine(reader *bufio.Reader) (string, error) {
	var buf strings.Builder

	for {
		l, more, err := reader.ReadLine()
		if err != nil {
			return "", err
		}

		buf.Write(l)

		if !more {
			break
		}
	}

	return buf.String(), nil
}

func readBlob(reader *bufio.Reader, size int) ([]byte, error) {
	data := make([]byte, size+2)

	_, err := io.ReadFull(reader, data)
	if err != nil {
		return nil, err
	}

	if !slices.Equal(data[size:], []byte(endLine)) {
		return nil, MissingLineEnd
	}

	return data[0:size], nil
}

func writeLine(writer io.Writer, cmd string, args ...any) error {
	_, err := fmt.Fprintf(writer, cmd, args...)

	return err
}
