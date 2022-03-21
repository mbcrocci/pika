package pika

import "strings"

func AppendToTopic(key, str string) string {
	str = strings.ReplaceAll(str, " ", "")
	str = strings.ReplaceAll(str, "-", "")
	str = strings.ReplaceAll(str, "_", "")
	str = strings.ToLower(str)

	return key + "." + str
}
