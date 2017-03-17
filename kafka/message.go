package kafka

import "sort"

const CRLF = "\r\n"

type FTMessage struct {
	Headers map[string]string
	Body    string
}

func (msg *FTMessage) String() string {
	builtMessage := "FTMSG/1.0" + CRLF

	var keys []string

	//order headers
	for header := range msg.Headers {
		keys = append(keys, header)
	}
	sort.Strings(keys)

	//set headers
	for _, key := range keys {
		builtMessage = builtMessage + key + ": " + msg.Headers[key] + CRLF
	}

	builtMessage = builtMessage + CRLF + msg.Body

	return builtMessage
}
