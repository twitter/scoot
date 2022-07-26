package errors

type ExitCode int

const (
	PreProcessingFailureExitCode ExitCode = 200

	// GitDB specific exit codes
	GenericCheckoutFailureExitCode = 210
	CleanFailureExitCode           = 211
	CheckoutFailureExitCode        = 212
	ReleaseCheckoutFailureCode     = 213
	BundleUploadFailureExitCode    = 214
	ReadFileAllFailureExitCode     = 215
	ExportGitCommitFailureExitCode = 216
	DBInitFailureExitCode          = 217

	LogRefCreationFailureExitCode = 220

	PostProcessingFailureExitCode = 230

	CouldNotExecExitCode                 = 240
	HighInitialMemoryUtilizationExitCode = 241

	PostExecFailureExitCode = 250
)
