package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"time"
	"github.com/jackc/pgx/v5"
)

const (
	MIGRATION_TABLE_NAME = "RG_MIGRATIONS"
)

func compare_int64_to_string(value_int int64, value_str *string) bool {
		parse_value, err := strconv.ParseInt(*value_str, 10, 64)
		if err != nil {
			log.Printf("compare_int64_to_string: %s\n", err.Error())
			return false
		}
		return value_int == parse_value
}

func getConnection(connString *string) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), *connString)
	if err != nil {
		log.Fatalf("Could not connect to the database: %s\n", err.Error())
		os.Exit(1)
	}
	return conn
}

func isDuplicate(filename *string, new_filename *string) {
	// the timestamp is 10 characters, then you have the underscore at position 11.
	// To access the first actual character of the name use 11 cause we start counting from 0, 
	// unfortunately this is not Ada :(
	name_length := len(*new_filename)
	if (*filename)[11:11+name_length] == *new_filename {
		log.Fatalf("Duplicate migration name!")
		os.Exit(1)
	}
}

// check if the name provided is a duplicate
func checkDuplicate(migrationsFolder *string, name *string) {
	// loop through the migration directory
	entries, err := os.ReadDir(*migrationsFolder)
	if err != nil {
		log.Fatalf("Error encountered%s", err.Error())
		os.Exit(1)
	}

	for _, entry := range entries {
		full_filename := entry.Name()
		// now we need to extract the name
		isDuplicate(&full_filename, name)
	}
}

func createMigrationsFiles(migrationsFolder *string, name *string) (*string, *string) {
	// check if the migration name has already been assigned to another migration.
	checkDuplicate(migrationsFolder, name)

	// first generate a timestamp
	var ts int64 = time.Now().Unix()

	// concatanate timestamp with migration name in the following format
	// <timestamp>_<filenam>.<up|down>.sql
	up_migration := fmt.Sprintf("%s/%d_%s.up.sql", *migrationsFolder, ts, *name)
	down_migration := fmt.Sprintf("%s/%d_%s.down.sql", *migrationsFolder, ts, *name) 

	// after generating the path, create the actual files in the filesystem
	_, err1 := os.Create(up_migration)
	_, err2 := os.Create(down_migration)

	if err1 != nil || err2 != nil {
		log.Fatal(fmt.Sprintf("could not create the migrations files."))
		// if an error is encounter, delete one / both of the files
		// transaction-like style. If one fails both fail.
		if err1 == nil {
			os.Remove(up_migration)
		}
		if err2 == nil {
			os.Remove(down_migration)
		}
		os.Exit(1)
	}
	return &up_migration, &down_migration 
}

type timestamp_name struct {
	name 					*string
	timestamp 		int64
}

func extract_timestamps_filename_structs(migrations_path *string) ([]timestamp_name, error) {
	entries, err := os.ReadDir(*migrations_path)
	if err != nil {
		log.Fatalf("Error encountered%s", err.Error())
		return nil, err
	}

	// if there are no files, exit fast
	if len(entries) == 0 {
 		log.Println("Migrations directory is empty")
		os.Exit(0)
	}

	timestamps := []timestamp_name{}
	for _, entry := range entries {
		full_filename := entry.Name()
		full_length_filename := len(full_filename)
		// we want to only handle up migrations. At the end of the day they are the same
		// so we do not consider half of the files
		if full_filename[full_length_filename - 5] != 'p' {
			continue
		}
		// extract the first 10 characters of the string, the timestamp
		str_timestamp := full_filename[:10]
		// convert it to an integer for comparing it
		timestamp_int, err := strconv.ParseInt(str_timestamp, 10, 64)
		if err != nil {
			log.Fatalf("timestamp cannot be converted to int64: %s\n", str_timestamp)
			os.Exit(1)
		}
		// extract the filename
		filename := full_filename[11:11+full_length_filename - 7 - 11]
		// append the struct
		timestamps = append(timestamps, timestamp_name{ timestamp : timestamp_int, name:&filename })
	}
	return timestamps, nil
}

// remove the latest migration file created
func RemoveCommand(migrationsFolder *string) {	
	// first read all the files in the migrations folder
	entries, err := os.ReadDir(*migrationsFolder)
	if err != nil {
		log.Fatalf("Error encountered%s", err.Error())
		os.Exit(1)
	}

	// if there are no files, exit fast
	if len(entries) == 0 {
 		log.Println("Migrations directory is empty")
		os.Exit(0)
	}
	// this is a small structure that we will create to keep track of filename and timestamp
	// used when reconstructing the files
	type _ts struct {
		timestamp int64
		name 			*string
	}

	timestamps := []_ts{}
	for _, entry := range entries {
		full_filename := entry.Name()
		full_length_filename := len(full_filename)
		// we want to only handle up migrations. At the end of the day they are the same
		// so we do not consider half of the files
		if full_filename[full_length_filename - 5] != 'p' {
			continue
		}
		// extract the first 10 characters of the string, the timestamp
		str_timestamp := full_filename[:10]
		// convert it to an integer for comparing it
		timestamp_int, err := strconv.ParseInt(str_timestamp, 10, 64)
		if err != nil {
			log.Fatalf("timestamp cannot be converted to int64: %s\n", str_timestamp)
			os.Exit(1)
		}
		// extract the filename
		filename := full_filename[11:11+full_length_filename - 7 - 11]
		// append the struct
		timestamps = append(timestamps, _ts{ timestamp : timestamp_int, name:&filename })
	}

	// now we want to find the highest timestamp
	// using a very simple O(n) algorithm should do the trick
	max_index := 0
	for i, ts := range timestamps {
  	if ts.timestamp > timestamps[max_index].timestamp {
			max_index = i
		}
	}

	timestamp_max := timestamps[max_index].timestamp
	name_max := timestamps[max_index].name

	// now having the index we can finally remove the files found
	up_migration_filename := fmt.Sprintf("%s/%d_%s.up.sql", *migrationsFolder, timestamp_max , *name_max)
	down_migration_filename := fmt.Sprintf("%s/%d_%s.down.sql", *migrationsFolder, timestamp_max , *name_max)
	err_up := os.Remove(up_migration_filename)
	if err_up != nil {
		log.Fatalf("Could not remove the up migration. Remove the file %s manually!!!", up_migration_filename)
	}
	err_down := os.Remove(down_migration_filename)
	if err_down != nil {
		log.Fatalf("Could not remove the down migration. Remove the file %s manually!!!", down_migration_filename)
		if err_up != nil {
			os.Exit(1)
		}
	}
	fmt.Printf("successfully removed migrations %s, %s\n", up_migration_filename, down_migration_filename)
}

func CreateCommand(migrationsFolder *string, name *string) {
	up, down := createMigrationsFiles(migrationsFolder, name)
	fmt.Printf("Successfully created migration files: %s | %s\n", *up, *down)
	os.Exit(0)
}

func is_migration_empty(migration_path *string) (bool, error) {
	// we check if the migrations file are empty
	// if they are, then it does not make sense to add them.
	content, err := os.ReadFile(*migration_path)
	if err != nil {
		log.Fatalf("Could not read migration file: %s\n", err.Error())
		return false, err	
	}
	return len(content) <= 0, nil
}

// update the database
func UpdateCommand(connString *string, migrationsFolderPath *string) {
	conn := getConnection(connString)

	exists := check_if_migration_table_exists(conn)
	if !exists {
		create_migration_table(conn)
	}

	
	// now get all the migrations that have not been applied
	migrations_not_applied, err := get_not_applied_migrations(conn, migrationsFolderPath)
	if err != nil {
		log.Fatalf("Error: %s\n", err.Error())
		os.Exit(1)
	}
	// for _, m := range migrations_not_applied {
	// fmt.Printf("%+v\n", m)
	// }
	// now that we have the timestamp_name objects with the migrations that have not been applied
	// we can finally create the entries in the migration table
	// and apply the up migrations which are going to be at <migrations_dir>/<timestamp>_<name>.up.sql
	// both the migration table row and the actual migration need to be wrapped in a transaction
	// if one of them fails for whatever reason both fail
	for _, migration_not_applied := range migrations_not_applied {
		// we need to start a transaction
		tx, err := conn.Begin(context.Background())
		if err != nil {
			log.Fatalf("Database transaction error: %s\n", err.Error())
		}
		defer tx.Rollback(context.Background())
		cmd, err := create_migration_row(conn, migrationsFolderPath, &migration_not_applied)
		if err != nil {
		  os.Exit(1)
		}

		fmt.Println(cmd)

		// execute the content of the up sql file
		content_bytes, err := os.ReadFile(get_up_filename(migrationsFolderPath, &migration_not_applied))
		if err != nil {
				log.Fatalf("[UpdateCommand] could not read up filename\n")
				os.Exit(1)
		}
		content_str := string(content_bytes[:])
		cmd_2, err := conn.Exec(context.Background(), content_str)
		if err != nil {
			log.Fatalf("[UpdateCommand] Could not run up script: %s\n", err.Error())
			os.Exit(1)
		}
		log.Printf(cmd_2.String())
    tx.Commit(context.Background())	
	}	
}

// rollback the database
func RollbackCommand(connString *string, migrationsFolderPath *string) {
	// get the last migration that we have performed
	conn := getConnection(connString)
	last_migration, err := get_last_migration_row(conn)
	if err != nil {
		log.Fatalf("[RollbackCommand] Could not fetch latest migration: %s\n", err.Error())
		os.Exit(1)
	}
	// now that we have the last applied migration
	// we want to open a transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		log.Fatalf("[RollbackCommand] Could not open a transaction: %s\n", err.Error())
		os.Exit(1)
	}
	defer tx.Rollback(context.Background())
	// now we need to apply the rollback script
	cmd, err := conn.Exec(context.Background(), *last_migration.rollback_script)
	if err != nil {
		log.Fatalf("[RollbackCommand] Could not apply rollback script: %s\n", err.Error())
	}
	fmt.Println(cmd.String())
	// remove the migration row from the migration table

	remove_migration_output, err := remove_migration_from_db(conn, *last_migration.id)
	if err != nil {
		log.Fatalf("[RollbackCommand] could not remove row from migration table: %s\n", err.Error())
		os.Exit(1)
	}

	commit_error := tx.Commit(context.Background())
	if commit_error != nil {
		log.Fatalf("[RollbackCommand] Commit transaction error: %s\n", commit_error.Error())
	}
	// print the command now
	log.Println(remove_migration_output)
}

func remove_migration_from_db(conn *pgx.Conn, id int32) (string, error) {
	// remove the migration from the db
	sql := fmt.Sprintf(`
		DELETE FROM %s WHERE ID = $1;
	`, MIGRATION_TABLE_NAME)
	cmd, err := conn.Exec(context.Background(), sql, id)
	if err != nil {
		return "", nil
	}
	return cmd.String(), nil
}

// get the last migration that has been performed
func get_last_migration_row(conn *pgx.Conn) (*migration_obj, error) {
		var migration migration_obj 	
		sql := fmt.Sprintf(`
			SELECT id, name, version, checksum, applied_at, rollback_script
			FROM %s 
			ORDER BY ID DESC
			LIMIT 1;
		`, MIGRATION_TABLE_NAME)
		err := conn.QueryRow(context.Background(), sql).Scan(&migration.id, &migration.name, &migration.version,
							 						&migration.checksum, &migration.applied_at, &migration.rollback_script)
		if err != nil {
			return nil, err
		}
	return &migration, nil;
}

func check_if_migration_table_exists(conn *pgx.Conn) bool {
	sql := `
	SELECT EXISTS (
		SELECT 1 FROM pg_tables where tablename = $1
	) as table_existence;
	`
	var exists bool;
	err := conn.QueryRow(context.Background(), sql, MIGRATION_TABLE_NAME).Scan(&exists)
	if err != nil {
		log.Fatalf("Query error: %s\n", err.Error())
		os.Exit(1)
	}
	return exists
}

type migration_obj struct {
	id								*int32
	name							*string
	version						*int64
	checksum					*string
	applied_at				*time.Time
	rollback_script 	*string
}

func get_not_applied_migrations(conn *pgx.Conn, migrations_dir *string) ([]timestamp_name, error) {
	// fetch the ids of the migrations that have been applied
	sql := fmt.Sprintf(`
		SELECT version from %s;
	`, MIGRATION_TABLE_NAME)
	versions := []int64{}

	// loop through the rows and get the values
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		log.Fatalf("Query error: %s", err.Error())
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
		var version int64
		err = rows.Scan(&version) 
		if err != nil {
			log.Fatalf("Row Scan error: %s", err.Error())
			os.Exit(1)
		}
		versions = append(versions, version)
	}

	// Any errors encountered by rows.Next or rows.Scan will be returned here
	if rows.Err() != nil {
		log.Fatalf("Row Scan error: %s", err.Error())
		os.Exit(1)
	}

	// having the versions,  return the timestamp_name objects
	all_timestamp_filenames, err := extract_timestamps_filename_structs(migrations_dir) 
	if err != nil {
		log.Fatalf("Error encountered: %s\n", err.Error())
		os.Exit(1)
	}
	var timestamp_name_not_applied []timestamp_name
	// now loop through all the migration files and check if the migraitons
	for _, ts_name := range all_timestamp_filenames {
		ts := ts_name.timestamp
		// check if the timestamp is contained in the version list
		if !slices.Contains(versions, ts) {
			timestamp_name_not_applied = append(timestamp_name_not_applied, ts_name)
		}
	}
	return timestamp_name_not_applied, nil
}

func create_migration_table(conn *pgx.Conn) error {
	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id 									serial 						primary key,
			name 								varchar(64) 			not null,
			version 						bigint 						not null,
			checksum 						varchar(256) 			not null,
			applied_at    			timestamp 				not null,
			rollback_script  		text 							not null
		);	
	`, MIGRATION_TABLE_NAME)
	_, err := conn.Exec(context.Background(), sql)
	if err != nil {
		log.Fatalf("Connection error: %s\n", err.Error())
	}
	return err
}

func get_migration_filepath(migrations_dir *string, ts_name *timestamp_name, direction string) string {
	// direction is either "up" or "down"
	switch direction {
	case "up" : {
			return get_up_filename(migrations_dir, ts_name)
		}
		case "down": {
			return get_down_filename(migrations_dir, ts_name)
		}
		default: {
			log.Fatalf("get_migration_filepath only supports \"up\" and \"down\" for directions. Provided %s", direction)
		}
	}
	return fmt.Sprintf("%s/%d_%s.up.sql", *migrations_dir, ts_name.timestamp, *ts_name.name)
}

func get_up_filename(migrations_dir *string, ts_name *timestamp_name) string {
	return fmt.Sprintf("%s/%d_%s.up.sql", *migrations_dir, ts_name.timestamp, *ts_name.name)
}

func get_down_filename(migrations_dir *string, ts_name *timestamp_name) string {
	return fmt.Sprintf("%s/%d_%s.down.sql", *migrations_dir, ts_name.timestamp, *ts_name.name)
}

// create a row in the migration table
func create_migration_row(conn *pgx.Conn, migrations_folder *string, ts_name *timestamp_name) (string, error) {
	up_path := get_migration_filepath(migrations_folder, ts_name, "up")
	down_path := get_migration_filepath(migrations_folder, ts_name, "down")

	// check if both migrations are non-empty
	is_empty, err := is_migration_empty(&up_path)

	if err != nil {
		log.Fatalf("[create_migration_row] could not read up migration file: %s\n", err.Error())
		os.Exit(1)
	}

	if is_empty {
		log.Fatalf("[create_migration_row] migration up file %s cannot be empty\n", up_path)
		os.Exit(1)
	}

	is_empty, err = is_migration_empty(&down_path)
	if err != nil {
		log.Fatalf("[create_migration_row] could not read down migration file: %s\n", err.Error())
		os.Exit(1)
	}

	if is_empty {
		log.Fatalf("[create_migration_row] migration down file %s cannot be empty\n", down_path)
		os.Exit(1)
	}

	// first get the checksum of the filename
	checksum, err := calculate_checksum(up_path)
	if err != nil {
		log.Fatalf("Could not calculate checksum: %s\n", err.Error())
		os.Exit(1)
	}
	// now get the rollback script
	rollback_script, err_rollback := os.ReadFile(down_path)
	if err_rollback != nil {
		log.Fatalf("Could not read rollback script: %s\n", err.Error())
		os.Exit(1)
	}

	applied_at := time.Now()

	sql := fmt.Sprintf(`
		INSERT INTO %s(name, version, checksum, rollback_script, applied_at)
		VALUES($1, $2, $3, $4, $5)
	`, MIGRATION_TABLE_NAME)
 
	cmd, err_db := conn.Exec(context.Background(), sql, *ts_name.name, ts_name.timestamp, checksum, rollback_script, applied_at)
	if err_db != nil {
		log.Fatalf("Could not create migration row: %s\n", err_db.Error())
		return "", err_db
	}
	return cmd.String(), nil
}
