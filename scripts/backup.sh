#!/bin/bash

# Backup script for File Server
# Usage: ./backup.sh [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BACKUP_TYPE="full"
OUTPUT_DIR=""
COMPRESS=true
CLEANUP=true
RETENTION_DAYS=30
INCLUDE_CONFIG=true
INCLUDE_LOGS=false
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Function to print colored output
print_color() {
    echo -e "${1}${2}${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --type TYPE         Backup type (full|database|files|config) [default: full]"
    echo "  --output DIR        Output directory for backup [default: data/backups]"
    echo "  --no-compress       Don't compress backup files"
    echo "  --no-cleanup        Don't cleanup old backups"
    echo "  --retention DAYS    Retention period in days [default: 30]"
    echo "  --include-logs      Include log files in backup"
    echo "  --no-config         Don't include configuration files"
    echo "  -h, --help          Show this help message"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --type)
            BACKUP_TYPE="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --no-compress)
            COMPRESS=false
            shift
            ;;
        --no-cleanup)
            CLEANUP=false
            shift
            ;;
        --retention)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        --include-logs)
            INCLUDE_LOGS=true
            shift
            ;;
        --no-config)
            INCLUDE_CONFIG=false
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_color $RED "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Script directory and project paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Set default output directory
if [[ -z "$OUTPUT_DIR" ]]; then
    OUTPUT_DIR="$PROJECT_DIR/data/backups"
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to log messages
log_message() {
    local level=$1
    local message=$2
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] $message"
}

# Function to get file size in human readable format
get_file_size() {
    local file="$1"
    if [[ -f "$file" ]]; then
        if command -v numfmt >/dev/null 2>&1; then
            numfmt --to=iec-i --suffix=B $(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
        else
            du -h "$file" | cut -f1
        fi
    else
        echo "0B"
    fi
}

# Function to compress file if requested
compress_file() {
    local file="$1"
    local output="$2"
    
    if [[ "$COMPRESS" == true ]]; then
        print_color $YELLOW "Compressing backup..."
        if command -v gzip >/dev/null 2>&1; then
            gzip -c "$file" > "${output}.gz"
            rm "$file"
            echo "${output}.gz"
        else
            mv "$file" "$output"
            echo "$output"
        fi
    else
        mv "$file" "$output"
        echo "$output"
    fi
}

# Function to backup database
backup_database() {
    local db_file="$PROJECT_DIR/data/fileserver.db"
    local temp_backup="$OUTPUT_DIR/temp_db_backup_${TIMESTAMP}.db"
    local final_backup="$OUTPUT_DIR/database_backup_${TIMESTAMP}.db"
    
    if [[ ! -f "$db_file" ]]; then
        print_color $YELLOW "Database file not found: $db_file"
        return 0
    fi
    
    print_color $YELLOW "Backing up database..."
    
    # Use SQLite backup command if available, otherwise copy file
    if command -v sqlite3 >/dev/null 2>&1; then
        sqlite3 "$db_file" ".backup $temp_backup"
    else
        cp "$db_file" "$temp_backup"
    fi
    
    local compressed_file=$(compress_file "$temp_backup" "$final_backup")
    local file_size=$(get_file_size "$compressed_file")
    
    print_color $GREEN "✓ Database backup completed: $compressed_file ($file_size)"
    log_message "SUCCESS" "Database backup: $compressed_file ($file_size)"
}

# Function to backup files
backup_files() {
    local files_dir="$PROJECT_DIR/data/files"
    local temp_backup="$OUTPUT_DIR/temp_files_backup_${TIMESTAMP}.tar"
    local final_backup="$OUTPUT_DIR/files_backup_${TIMESTAMP}.tar"
    
    if [[ ! -d "$files_dir" ]]; then
        print_color $YELLOW "Files directory not found: $files_dir"
        return 0
    fi
    
    print_color $YELLOW "Backing up files..."
    
    # Create tar archive
    tar -cf "$temp_backup" -C "$(dirname "$files_dir")" "$(basename "$files_dir")"
    
    local compressed_file=$(compress_file "$temp_backup" "$final_backup")
    local file_size=$(get_file_size "$compressed_file")
    
    print_color $GREEN "✓ Files backup completed: $compressed_file ($file_size)"
    log_message "SUCCESS" "Files backup: $compressed_file ($file_size)"
}

# Function to backup configuration
backup_config() {
    local config_dir="$PROJECT_DIR/config"
    local temp_backup="$OUTPUT_DIR/temp_config_backup_${TIMESTAMP}.tar"
    local final_backup="$OUTPUT_DIR/config_backup_${TIMESTAMP}.tar"
    
    if [[ ! -d "$config_dir" ]]; then
        print_color $YELLOW "Config directory not found: $config_dir"
        return 0
    fi
    
    print_color $YELLOW "Backing up configuration..."
    
    # Create tar archive excluding sensitive files
    tar -cf "$temp_backup" -C "$(dirname "$config_dir")" \
        --exclude="*.key" --exclude="*.pem" --exclude="ssl/*" \
        "$(basename "$config_dir")"
    
    local compressed_file=$(compress_file "$temp_backup" "$final_backup")
    local file_size=$(get_file_size "$compressed_file")
    
    print_color $GREEN "✓ Configuration backup completed: $compressed_file ($file_size)"
    log_message "SUCCESS" "Configuration backup: $compressed_file ($file_size)"
}

# Function to backup logs
backup_logs() {
    local logs_dir="$PROJECT_DIR/logs"
    local temp_backup="$OUTPUT_DIR/temp_logs_backup_${TIMESTAMP}.tar"
    local final_backup="$OUTPUT_DIR/logs_backup_${TIMESTAMP}.tar"
    
    if [[ ! -d "$logs_dir" ]]; then
        print_color $YELLOW "Logs directory not found: $logs_dir"
        return 0
    fi
    
    print_color $YELLOW "Backing up logs..."
    
    # Create tar archive
    tar -cf "$temp_backup" -C "$(dirname "$logs_dir")" "$(basename "$logs_dir")"
    
    local compressed_file=$(compress_file "$temp_backup" "$final_backup")
    local file_size=$(get_file_size "$compressed_file")
    
    print_color $GREEN "✓ Logs backup completed: $compressed_file ($file_size)"
    log_message "SUCCESS" "Logs backup: $compressed_file ($file_size)"
}

# Function to perform full backup
backup_full() {
    local temp_backup="$OUTPUT_DIR/temp_full_backup_${TIMESTAMP}.tar"
    local final_backup="$OUTPUT_DIR/full_backup_${TIMESTAMP}.tar"
    
    print_color $YELLOW "Creating full backup..."
    
    # Create list of directories to backup
    local backup_items=()
    
    [[ -d "$PROJECT_DIR/data" ]] && backup_items+=("data")
    [[ "$INCLUDE_CONFIG" == true && -d "$PROJECT_DIR/config" ]] && backup_items+=("config")
    [[ "$INCLUDE_LOGS" == true && -d "$PROJECT_DIR/logs" ]] && backup_items+=("logs")
    
    if [[ ${#backup_items[@]} -eq 0 ]]; then
        print_color $YELLOW "No items found to backup"
        return 0
    fi
    
    # Create tar archive
    tar -cf "$temp_backup" -C "$PROJECT_DIR" \
        --exclude="data/backups" --exclude="*.tmp" --exclude="*.temp" \
        --exclude="config/ssl/*.key" --exclude="config/ssl/*.pem" \
        "${backup_items[@]}"
    
    local compressed_file=$(compress_file "$temp_backup" "$final_backup")
    local file_size=$(get_file_size "$compressed_file")
    
    print_color $GREEN "✓ Full backup completed: $compressed_file ($file_size)"
    log_message "SUCCESS" "Full backup: $compressed_file ($file_size)"
}

# Function to cleanup old backups
cleanup_old_backups() {
    if [[ "$CLEANUP" != true ]]; then
        return 0
    fi
    
    print_color $YELLOW "Cleaning up old backups (older than $RETENTION_DAYS days)..."
    
    local deleted_count=0
    while IFS= read -r -d '' file; do
        rm "$file"
        ((deleted_count++))
        log_message "INFO" "Deleted old backup: $file"
    done < <(find "$OUTPUT_DIR" -name "*backup_*" -type f -mtime +$RETENTION_DAYS -print0 2>/dev/null || true)
    
    if [[ $deleted_count -gt 0 ]]; then
        print_color $GREEN "✓ Cleaned up $deleted_count old backup(s)"
    else
        print_color $BLUE "No old backups to cleanup"
    fi
}

# Main backup function
main() {
    print_color $BLUE "File Server Backup Script"
    print_color $BLUE "========================"
    echo "Backup type: $BACKUP_TYPE"
    echo "Output directory: $OUTPUT_DIR"
    echo "Compression: $COMPRESS"
    echo "Include config: $INCLUDE_CONFIG"
    echo "Include logs: $INCLUDE_LOGS"
    echo "Retention: $RETENTION_DAYS days"
    echo
    
    log_message "INFO" "Starting $BACKUP_TYPE backup"
    
    # Perform backup based on type
    case $BACKUP_TYPE in
        "database")
            backup_database
            ;;
        "files")
            backup_files
            ;;
        "config")
            backup_config
            ;;
        "logs")
            backup_logs
            ;;
        "full")
            backup_full
            ;;
        *)
            print_color $RED "Invalid backup type: $BACKUP_TYPE"
            show_usage
            exit 1
            ;;
    esac
    
    # Cleanup old backups
    cleanup_old_backups
    
    print_color $GREEN "✓ Backup operation completed successfully!"
    log_message "SUCCESS" "Backup operation completed"
    
    # Show backup summary
    print_color $BLUE "Backup summary:"
    ls -lah "$OUTPUT_DIR"/*backup*${TIMESTAMP}* 2>/dev/null || echo "No backup files created"
}

# Run main backup
main "$@"