import os
import importlib
from identification import ( # Import discover_schema here
    discover_schema,
    analyze_queries_dynamic,
    check_indexes_dynamic,
    check_data_integrity_dynamic,
    check_security_dynamic,
    analyze_triggers_dynamic,
    analyze_relationships_performance
)
from visualization import generate_html_report # Assuming visualization.py is in the root

# Placeholder for the selected database handler module
db_handler = None

if __name__ == '__main__':
    print("Starting dynamic database analysis tool...")

    db_paths = None # Initialize db_paths (for SQLite file paths) or connection details (for MySQL)
    discovered_schema = {} # Initialize discovered_schema

    while True:
        db_type_choice = input("Select database type: (S)QLite or (M)ySQL? (S/M): ").strip().upper()
        if db_type_choice == 'S':
            db_handler = importlib.import_module('db_handlers.sqlite_handler')
            print("SQLite handler loaded.")
            db_handler.setup_sample_database() # Call setup from the selected handler
            # For sample, db_paths remains None, get_all_shard_engines will use defaults
            print("Sample database setup complete. Proceeding with analysis.")
            
            # Discover schema for the newly created sample database
            # Call discover_schema from identification.py, passing the db_handler and None for paths
            discovered_schema = discover_schema(db_handler, None) 
            if not discovered_schema['shards']:
                print("Error: Could not discover schema for sample database. Exiting.")
                exit()
            break
        elif db_type_choice == 'M':
            db_handler = importlib.import_module('db_handlers.mysql_handler')
            print("MySQL handler loaded.")
            db_handler.setup_sample_database() # Provide setup instructions for MySQL

            mysql_conn_details = []
            num_shards_str = input("Enter the number of MySQL databases/shards you want to analyze: ").strip()
            try:
                num_shards = int(num_shards_str)
                if num_shards <= 0:
                    print("Number of shards must be positive.")
                    continue
            except ValueError:
                print("Invalid number. Please enter an integer.")
                continue

            print("\nEnter MySQL connection details for each database/shard:")
            for i in range(num_shards):
                print(f"\n--- Shard {i+1} ---")
                host = input(f"Host (e.g., localhost): ").strip()
                port_str = input(f"Port (e.g., 3306): ").strip()
                user = input(f"User: ").strip()
                password = input(f"Password: ").strip()
                db_name = input(f"Database Name: ").strip()
                
                try:
                    port = int(port_str)
                except ValueError:
                    print("Invalid port. Please enter an integer.")
                    continue

                mysql_conn_details.append({
                    'host': host,
                    'port': port,
                    'user': user,
                    'password': password,
                    'db_name': db_name
                })
            
            # For MySQL, mysql_conn_details is the 'db_paths' equivalent for get_all_shard_engines
            db_paths = mysql_conn_details 
            
            # Discover schema for the existing databases
            # Call discover_schema from identification.py, passing the db_handler and connection details
            discovered_schema = discover_schema(db_handler, db_paths)
            if not discovered_schema['shards']:
                print("Error: Could not discover schema for existing databases. Exiting.")
                exit()

            print("Existing database connections accepted and schema discovered. Proceeding with analysis.")
            break
        else:
            print("Invalid choice. Please enter 'S' or 'M'.")

    # Pass the selected db_handler and discovered_schema, AND db_paths (connection_details) to all analysis functions
    print("\nAnalyzing query performance dynamically...")
    query_performance_data = analyze_queries_dynamic(db_handler, discovered_schema, db_paths)
    
    print("\nChecking for missing and redundant indexes dynamically...")
    index_issues, index_suggestions = check_indexes_dynamic(db_handler, discovered_schema, db_paths)
    
    print("\nChecking for data integrity issues dynamically...")
    integrity_issues = check_data_integrity_dynamic(db_handler, discovered_schema, db_paths)

    print("\nChecking password and sensitive data security dynamically...")
    security_findings = check_security_dynamic(db_handler, discovered_schema, db_paths)

    print("\nAnalyzing trigger performance dynamically...")
    trigger_performance_results = analyze_triggers_dynamic(db_handler, discovered_schema, db_paths)
    
    print("\nAnalyzing relationship (JOIN) performance dynamically...")
    relationship_perf_results = analyze_relationships_performance(db_handler, discovered_schema, db_paths)

    print("\nGenerating comprehensive HTML report...")
    # The visualization.py does not need the db_handler directly, as it only consumes the results
    html_report_content = generate_html_report(
        query_performance_data,
        index_issues,
        integrity_issues,
        security_findings,
        index_suggestions,
        trigger_performance_results,
        relationship_perf_results,
        discovered_schema
    )
    
    report_filename = 'database_report.html'
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(html_report_content)
    
    print(f"\nComprehensive database report saved as '{report_filename}'")
    print("\nDynamic database analysis complete.")
