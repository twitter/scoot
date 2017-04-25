package common

import (
	uuid "github.com/nu7hatch/gouuid"
)

func GenUUID() string {
	// generates a ClientId using a random uuid

	// uuid.NewV4() should never actually return an error the code uses
	// rand.Read Api to generate the uuid, which according to golang docs
	// "Read always returns ... a nil error" https://golang.org/pkg/math/rand/#Read
	for {
		if id, err := uuid.NewV4(); err == nil {
			return id.String()
		}
	}
}
