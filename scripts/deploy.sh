#!/bin/bash

# Deployment script for File Server
# Usage: ./deploy.sh [staging|production] [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="staging"
BUILD_TYPE="Release"
BACKUP_BEFORE_DEPLOY=true
RUN_MIGRATIONS=true
RESTART_SERVICE=true
HEALTH_CHECK=true
ROLLBACK_ON_FAILURE=true

# Function to print colored output
print_color() {
    echo -e "${1}${2}${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [environment] [options]"
    echo "Environments:"
    echo "  staging     Deploy to staging environment"
    echo "  production  Deploy to production environment"
    echo ""
    echo "Options:"
    echo "  --no-backup         Skip database backup before deployment"
    echo "  --no-migrations     Skip database migrations"
    echo "  --no-restart        Don't restart services after deployment"
    echo "  --no-health-check   Skip health check after deployment"
    echo "  --no-rollback       Don't rollback on deployment failure"
    echo "  --build-type TYPE   Build type (Debug|Release) [default: Release]"
    echo "  -h, --help          Show this help message"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        staging|production)
            ENVIRONMENT="$1"
            shift
            ;;
        --no-backup)
            BACKUP_BEFORE_DEPLOY=false
            shift
            ;;
        --no-migrations)
            RUN_MIGRATIONS=false
            shift
            ;;
        --no-restart)
            RESTART_SERVICE=false
            shift
            ;;
        --no-health-check)
            HEALTH_CHECK=false
            shift
            ;;
        --no-rollback)
            ROLLBACK_ON_FAILURE=false
            shift
            ;;
        --build-type)
            BUILD_TYPE="$2"
            shift 2
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
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
DEPLOY_LOG="$PROJECT_DIR/logs/deploy_${ENVIRONMENT}_${TIMESTAMP}.log"

# Environment-specific configuration
case $ENVIRONMENT in
    "staging")
        DEPLOY_PATH="/opt/fileserver/staging"
        SERVICE_NAME="fileserver-staging"
        CONFIG_FILE="staging.json"
        ;;
    "production")
        DEPLOY_PATH="/opt/fileserver/production"
        SERVICE_NAME="fileserver"
        CONFIG_FILE="production.json"
        ;;
    *)
        print_color $RED "Invalid environment: $ENVIRONMENT"
        exit 1
        ;;
esac

# Create log directory
mkdir -p "$(dirname "$DEPLOY_LOG")"

# Function to log messages
log_message() {
    local level=$1
    local message=$2
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] $message" | tee -a "$DEPLOY_LOG"
}

# Function to run command with logging
run_command() {
    local command="$1"
    local description="$2"
    
    print_color $YELLOW "Running: $description"
    log_message "INFO" "Executing: $command"
    
    if eval "$command" >> "$DEPLOY_LOG" 2>&1; then
        print_color $GREEN "✓ $description completed"
        log_message "SUCCESS" "$description completed"
        return 0
    else
        print_color $RED "✗ $description failed"
        log_message "ERROR" "$description failed"
        return 1
    fi
}

# Function to check if service is running
is_service_running() {
    systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null
}

# Function to perform health check
perform_health_check() {
    local max_attempts=30
    local attempt=1
    
    print_color $YELLOW "Performing health check..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -k -s "https://localhost:8443/health" > /dev/null 2>&1; then
            print_color $GREEN "✓ Health check passed"
            return 0
        fi
        
        print_color $YELLOW "Health check attempt $attempt/$max_attempts failed, retrying in 2 seconds..."
        sleep 2
        ((attempt++))
    done
    
    print_color $RED "✗ Health check failed after $max_attempts attempts"
    return 1
}

# Function to rollback deployment
rollback_deployment() {
    print_color $RED "Rolling back deployment..."
    log_message "WARNING" "Starting rollback procedure"
    
    # Stop current service
    if is_service_running; then
        run_command "sudo systemctl stop $SERVICE_NAME" "Stop current service"
    fi
    
    # Restore backup if exists
    if [[ -f "$DEPLOY_PATH/backup/fileserver_backup_${TIMESTAMP}" ]]; then
        run_command "sudo mv '$DEPLOY_PATH/backup/fileserver_backup_${TIMESTAMP}' '$DEPLOY_PATH/bin/file_server'" "Restore previous binary"
    fi
    
    # Restore database backup if exists
    if [[ "$BACKUP_BEFORE_DEPLOY" == true && -f "$PROJECT_DIR/data/backups/fileserver_${TIMESTAMP}.db" ]]; then
        run_command "sudo cp '$PROJECT_DIR/data/backups/fileserver_${TIMESTAMP}.db' '$PROJECT_DIR/data/fileserver.db'" "Restore database backup"
    fi
    
    # Start service
    run_command "sudo systemctl start $SERVICE_NAME" "Start service after rollback"
    
    print_color $YELLOW "Rollback completed"
    log_message "INFO" "Rollback procedure completed"
}

# Main deployment function
main() {
    print_color $BLUE "File Server Deployment Script"
    print_color $BLUE "============================="
    echo "Environment: $ENVIRONMENT"
    echo "Deploy path: $DEPLOY_PATH"
    echo "Service: $SERVICE_NAME"
    echo "Config: $CONFIG_FILE"
    echo "Build type: $BUILD_TYPE"
    echo "Log file: $DEPLOY_LOG"
    echo
    
    log_message "INFO" "Starting deployment to $ENVIRONMENT environment"
    
    # Check if we have necessary permissions
    if [[ ! -w "$DEPLOY_PATH" ]]; then
        print_color $RED "No write permission to deployment path: $DEPLOY_PATH"
        print_color $YELLOW "You may need to run with sudo or adjust permissions"
        exit 1
    fi
    
    # Build the application
    print_color $YELLOW "Building application..."
    if ! "$SCRIPT_DIR/build.sh" --type "$BUILD_TYPE" --no-tests; then
        print_color $RED "Build failed"
        exit 1
    fi
    
    # Create backup if requested
    if [[ "$BACKUP_BEFORE_DEPLOY" == true ]]; then
        mkdir -p "$DEPLOY_PATH/backup"
        if [[ -f "$DEPLOY_PATH/bin/file_server" ]]; then
            run_command "cp '$DEPLOY_PATH/bin/file_server' '$DEPLOY_PATH/backup/fileserver_backup_${TIMESTAMP}'" "Create binary backup"
        fi
        
        # Backup database
        if [[ -f "$PROJECT_DIR/data/fileserver.db" ]]; then
            run_command "$SCRIPT_DIR/backup.sh --output '$PROJECT_DIR/data/backups/fileserver_${TIMESTAMP}.db'" "Create database backup"
        fi
    fi
    
    # Stop service if running
    if is_service_running; then
        run_command "sudo systemctl stop $SERVICE_NAME" "Stop current service"
    fi
    
    # Deploy new binary
    run_command "sudo cp '$PROJECT_DIR/build-release/bin/file_server' '$DEPLOY_PATH/bin/'" "Deploy new binary"
    
    # Deploy configuration
    if [[ -f "$PROJECT_DIR/config/$CONFIG_FILE" ]]; then
        run_command "sudo cp '$PROJECT_DIR/config/$CONFIG_FILE' '$DEPLOY_PATH/config/server.json'" "Deploy configuration"
    fi
    
    # Set permissions
    run_command "sudo chown -R fileserver:fileserver '$DEPLOY_PATH'" "Set file ownership"
    run_command "sudo chmod +x '$DEPLOY_PATH/bin/file_server'" "Set executable permissions"
    
    # Run database migrations
    if [[ "$RUN_MIGRATIONS" == true ]]; then
        run_command "sudo -u fileserver '$DEPLOY_PATH/bin/file_server' --migrate --config '$DEPLOY_PATH/config/server.json'" "Run database migrations"
    fi
    
    # Start service
    if [[ "$RESTART_SERVICE" == true ]]; then
        run_command "sudo systemctl start $SERVICE_NAME" "Start service"
        run_command "sudo systemctl enable $SERVICE_NAME" "Enable service"
    fi
    
    # Perform health check
    if [[ "$HEALTH_CHECK" == true ]]; then
        if ! perform_health_check; then
            if [[ "$ROLLBACK_ON_FAILURE" == true ]]; then
                rollback_deployment
                exit 1
            else
                print_color $RED "Deployment completed but health check failed"
                log_message "WARNING" "Deployment completed but health check failed"
                exit 1
            fi
        fi
    fi
    
    print_color $GREEN "✓ Deployment to $ENVIRONMENT completed successfully!"
    log_message "SUCCESS" "Deployment to $ENVIRONMENT completed successfully"
    
    # Show service status
    print_color $BLUE "Service status:"
    sudo systemctl status "$SERVICE_NAME" --no-pager -l
}

# Trap function to handle script interruption
cleanup() {
    print_color $YELLOW "Deployment interrupted. Cleaning up..."
    log_message "WARNING" "Deployment interrupted by user"
    exit 1
}

trap cleanup INT TERM

# Run main deployment
main "$@"