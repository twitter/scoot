package errors

type ExitCode int

const (
	PreProcessingFailureExitCode ExitCode = 70

	// GitDB specific exit codes
	GenericCheckoutFailureExitCode = 80
	CleanFailureExitCode           = 81
	CheckoutFailureExitCode        = 82
	ReleaseCheckoutFailureExitCode = 83
	BundleUploadFailureExitCode    = 84
	ReadFileAllFailureExitCode     = 85
	ExportGitCommitFailureExitCode = 86
	DBInitFailureExitCode          = 87

	LogRefCreationFailureExitCode = 90

	PostProcessingFailureExitCode = 100

	CouldNotExecExitCode = 110

	PostExecFailureExitCode = 120
)
