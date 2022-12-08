package exec

var attributeTypeCast = map[string]string{
	"array":    "ss",
	"strings":  "ss",
	"int":      "n",
	"decimal":  "n",
	"ints":     "ns",
	"decimals": "ns",
	"map":      "m",
}

var nativeFunctions = map[string]bool{
	"contains":       true,
	"begins_with":    true,
	"attribute_type": true,
	"size":           true,
}
