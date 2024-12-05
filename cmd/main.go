package main

import (
	"fmt"
	"flag"
	"os"
	"log"
	"github.com/adrenaissance/renegator/internal"
)

func main() {
	// Check if a command is provided
	if len(os.Args) < 2 {
		fmt.Println("Usage: renegator <command> [options]")
		os.Exit(1)
	}
	
	migrationsFolder := flag.String("folder", "./migrations", "Folder where migrations are stored. Default './migrations'.")
	connString := flag.String("conn", "", "database connection string")
	flag.Parse()

	fmt.Println(*connString)

	if migrationsFolder == nil {
		log.Fatal("migrations folder cannot be null")
		os.Exit(1)
	}
  err := internal.CheckMigrationsFolder(migrationsFolder)
	if err != nil {
			log.Fatal(err.Error())
			os.Exit(1)
	}
	switch flag.Arg(0) {
	case "create":
		if len(os.Args) >= 3 {
			filename := flag.Arg(1)
			internal.CreateCommand(migrationsFolder, &filename)
		} else {
			fmt.Println("The \"create\" command requires a filename: e.g. renegator create my_filename")
			os.Exit(1)
		}
	case "update":
		internal.UpdateCommand(connString, migrationsFolder)

	case "remove":
		internal.RemoveCommand(migrationsFolder)

	case "rollback":
		internal.RollbackCommand(connString, migrationsFolder)

	default:
		fmt.Printf("Unknown command: %s\n", flag.Arg(1))
		fmt.Println("Available commands: update, rollback")
		os.Exit(1)
	}
}
