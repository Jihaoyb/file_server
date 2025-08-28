/**
 * Simple File Server Client Example
 * 
 * This example demonstrates how to:
 * 1. Connect to the file server
 * 2. Authenticate with username/password
 * 3. Upload a file
 * 4. Download a file
 * 5. List files
 * 6. Search for files
 */

#include <iostream>
#include <string>
#include <fstream>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/StreamCopier.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/URI.h>
#include <Poco/Base64Encoder.h>

using namespace Poco::Net;
using namespace Poco::JSON;
using namespace std;

class FileServerClient {
private:
    string server_url;
    string auth_token;
    HTTPSClientSession session;
    
public:
    FileServerClient(const string& url) : server_url(url) {
        Poco::URI uri(url);
        session.setHost(uri.getHost());
        session.setPort(uri.getPort());
        
        // For development, accept self-signed certificates
        session.setVerifyMode(Context::VERIFY_NONE);
    }
    
    /**
     * Authenticate with the server using username and password
     */
    bool login(const string& username, const string& password) {
        try {
            HTTPRequest request(HTTPRequest::HTTP_POST, "/api/v1/auth/login");
            request.setContentType("application/json");
            
            // Create login JSON
            Object::Ptr login_data = new Object;
            login_data->set("username", username);
            login_data->set("password", password);
            
            stringstream json_stream;
            login_data->stringify(json_stream);
            string json_body = json_stream.str();
            
            request.setContentLength(json_body.length());
            
            ostream& request_stream = session.sendRequest(request);
            request_stream << json_body;
            
            HTTPResponse response;
            istream& response_stream = session.receiveResponse(response);
            
            if (response.getStatus() == HTTPResponse::HTTP_OK) {
                stringstream response_body;
                Poco::StreamCopier::copyStream(response_stream, response_body);
                
                Parser parser;
                Dynamic::Var result = parser.parse(response_body.str());
                Object::Ptr response_obj = result.extract<Object::Ptr>();
                
                auth_token = response_obj->getValue<string>("token");
                cout << "✓ Login successful" << endl;
                return true;
            } else {
                cout << "✗ Login failed: " << response.getReason() << endl;
                return false;
            }
        } catch (const exception& e) {
            cout << "✗ Login error: " << e.what() << endl;
            return false;
        }
    }
    
    /**
     * Upload a file to the server
     */
    bool uploadFile(const string& local_path, const string& description = "") {
        try {
            ifstream file(local_path, ios::binary);
            if (!file.is_open()) {
                cout << "✗ Cannot open file: " << local_path << endl;
                return false;
            }
            
            HTTPRequest request(HTTPRequest::HTTP_POST, "/api/v1/files");
            request.add("Authorization", "Bearer " + auth_token);
            request.setContentType("multipart/form-data; boundary=----boundary123");
            
            // Prepare multipart form data
            stringstream form_data;
            form_data << "------boundary123\r\n";
            form_data << "Content-Disposition: form-data; name=\"file\"; filename=\"" 
                      << Poco::Path(local_path).getFileName() << "\"\r\n";
            form_data << "Content-Type: application/octet-stream\r\n\r\n";
            
            // Read file content
            file.seekg(0, ios::end);
            size_t file_size = file.tellg();
            file.seekg(0, ios::beg);
            
            string file_content((istreambuf_iterator<char>(file)), istreambuf_iterator<char>());
            form_data << file_content << "\r\n";
            
            if (!description.empty()) {
                form_data << "------boundary123\r\n";
                form_data << "Content-Disposition: form-data; name=\"description\"\r\n\r\n";
                form_data << description << "\r\n";
            }
            
            form_data << "------boundary123--\r\n";
            
            string body = form_data.str();
            request.setContentLength(body.length());
            
            ostream& request_stream = session.sendRequest(request);
            request_stream << body;
            
            HTTPResponse response;
            istream& response_stream = session.receiveResponse(response);
            
            if (response.getStatus() == HTTPResponse::HTTP_OK || 
                response.getStatus() == HTTPResponse::HTTP_CREATED) {
                cout << "✓ File uploaded successfully: " << local_path << endl;
                return true;
            } else {
                cout << "✗ Upload failed: " << response.getReason() << endl;
                return false;
            }
        } catch (const exception& e) {
            cout << "✗ Upload error: " << e.what() << endl;
            return false;
        }
    }
    
    /**
     * List all files
     */
    void listFiles() {
        try {
            HTTPRequest request(HTTPRequest::HTTP_GET, "/api/v1/files");
            request.add("Authorization", "Bearer " + auth_token);
            
            session.sendRequest(request);
            
            HTTPResponse response;
            istream& response_stream = session.receiveResponse(response);
            
            if (response.getStatus() == HTTPResponse::HTTP_OK) {
                stringstream response_body;
                Poco::StreamCopier::copyStream(response_stream, response_body);
                
                Parser parser;
                Dynamic::Var result = parser.parse(response_body.str());
                Object::Ptr response_obj = result.extract<Object::Ptr>();
                
                cout << "📁 Files on server:" << endl;
                cout << "===================" << endl;
                
                // Parse and display files (this would depend on your actual API response format)
                cout << response_body.str() << endl;
                
            } else {
                cout << "✗ Failed to list files: " << response.getReason() << endl;
            }
        } catch (const exception& e) {
            cout << "✗ List files error: " << e.what() << endl;
        }
    }
    
    /**
     * Download a file by ID
     */
    bool downloadFile(const string& file_id, const string& output_path) {
        try {
            HTTPRequest request(HTTPRequest::HTTP_GET, "/api/v1/files/" + file_id + "/download");
            request.add("Authorization", "Bearer " + auth_token);
            
            session.sendRequest(request);
            
            HTTPResponse response;
            istream& response_stream = session.receiveResponse(response);
            
            if (response.getStatus() == HTTPResponse::HTTP_OK) {
                ofstream output_file(output_path, ios::binary);
                if (!output_file.is_open()) {
                    cout << "✗ Cannot create output file: " << output_path << endl;
                    return false;
                }
                
                Poco::StreamCopier::copyStream(response_stream, output_file);
                cout << "✓ File downloaded: " << output_path << endl;
                return true;
            } else {
                cout << "✗ Download failed: " << response.getReason() << endl;
                return false;
            }
        } catch (const exception& e) {
            cout << "✗ Download error: " << e.what() << endl;
            return false;
        }
    }
};

int main(int argc, char* argv[]) {
    cout << "File Server Client Example" << endl;
    cout << "=========================" << endl;
    
    // Default server URL (change this to match your server)
    string server_url = "https://localhost:8443";
    
    if (argc > 1) {
        server_url = argv[1];
    }
    
    FileServerClient client(server_url);
    
    // Get login credentials
    string username, password;
    cout << "Enter username: ";
    cin >> username;
    cout << "Enter password: ";
    cin >> password;
    
    // Login
    if (!client.login(username, password)) {
        cout << "Authentication failed. Exiting." << endl;
        return 1;
    }
    
    // Interactive menu
    while (true) {
        cout << "\n📋 Available actions:" << endl;
        cout << "1. Upload file" << endl;
        cout << "2. List files" << endl;
        cout << "3. Download file" << endl;
        cout << "4. Exit" << endl;
        cout << "Choose action (1-4): ";
        
        int choice;
        cin >> choice;
        cin.ignore(); // Consume newline
        
        switch (choice) {
            case 1: {
                string file_path, description;
                cout << "Enter file path to upload: ";
                getline(cin, file_path);
                cout << "Enter description (optional): ";
                getline(cin, description);
                client.uploadFile(file_path, description);
                break;
            }
            case 2:
                client.listFiles();
                break;
            case 3: {
                string file_id, output_path;
                cout << "Enter file ID to download: ";
                getline(cin, file_id);
                cout << "Enter output path: ";
                getline(cin, output_path);
                client.downloadFile(file_id, output_path);
                break;
            }
            case 4:
                cout << "Goodbye!" << endl;
                return 0;
            default:
                cout << "Invalid choice. Please try again." << endl;
        }
    }
    
    return 0;
}