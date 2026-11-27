package main

type command struct {
	run       func(current *process) int
	arguments bool
}

func commands() map[string]command {
	return map[string]command{
		commandAll: {
			run: allRole().run,
		},
		commandIngester: {
			run: ingesterRole().run,
		},
		commandProcessor: {
			run: processorRole().run,
		},
		commandTokens: {
			run:       tokensCommand,
			arguments: true,
		},
	}
}

func commandOf(args []string) (command, bool) {
	if len(args) == 0 {
		return command{}, false
	}
	selected, found := commands()[args[0]]
	if !found || selected.arguments != (len(args) > 1) {
		return command{}, false
	}
	return selected, true
}

func realMain(current *process) int {
	selected, found := commandOf(current.Args)
	if !found {
		return fail(current.Stderr, errUsage)
	}
	return selected.run(current)
}
