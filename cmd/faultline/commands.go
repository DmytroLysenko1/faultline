package main

type command struct {
	run func(current process) int
}

func commands() map[string]command {
	return map[string]command{
		commandAll: {
			run: runAll,
		},
		commandIngester: {
			run: runIngester,
		},
		commandProcessor: {
			run: runProcessor,
		},
	}
}

func commandOf(args []string) (command, bool) {
	if len(args) != 1 {
		return command{}, false
	}
	selected, found := commands()[args[0]]
	return selected, found
}

func runAll(current process) int {
	setup, err := start(current)
	if err != nil {
		return fail(current.Stderr, err)
	}
	return serve(current.Stderr, &setup)
}

func realMain(current process) int {
	selected, found := commandOf(current.Args)
	if !found {
		return fail(current.Stderr, errUsage)
	}
	return selected.run(current)
}
