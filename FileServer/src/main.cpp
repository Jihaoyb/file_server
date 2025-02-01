#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTMLForm.h"
#include "Poco/Net/HTTPBasicCredentials.h"
#include "Poco/Net/PartHandler.h"
#include "Poco/Exception.h"
#include "Poco/StreamCopier.h"
#include <boost/filesystem.hpp>
#include <memory>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <iostream>

const std::string ROOT_DIRECTORY = "C:/Users/yejih/Desktop/cloud_server";
const std::unordered_map<std::string, std::pair<std::string, std::string>> USERS = {
    {"admin", {"password", "admin"}},
    {"user", {"password", "user"}}
};

class FilePartHandler : public Poco::Net::PartHandler {
public:
    explicit FilePartHandler(const std::string &uploadDir) : _uploadDir(uploadDir) {}

    void handlePart(const Poco::Net::MessageHeader &header, std::istream &stream) override {
        if (header.has("Content-Disposition")) {
            std::string disposition = header["Content-Disposition"];
            size_t pos = disposition.find("filename=");
            if (pos != std::string::npos) {
                std::string filename = disposition.substr(pos + 10);
                filename = filename.substr(0, filename.size() - 1);
                _filePath = _uploadDir + "/" + filename;
                std::ofstream outfile(_filePath, std::ios::binary);
                if (!outfile.is_open()) {
                    throw Poco::IOException("Failed to open file for writing: " + _filePath);
                }
                Poco::StreamCopier::copyStream(stream, outfile);
                outfile.close();
            }
        }
    }

    const std::string &filePath() const { return _filePath; }

private:
    std::string _uploadDir;
    std::string _filePath;
};

class FileServerRequestHandler : public Poco::Net::HTTPRequestHandler {
public:
    void handleRequest(Poco::Net::HTTPServerRequest &request, Poco::Net::HTTPServerResponse &response) override {
        try {
            if (!authenticate(request)) {
                sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED, "Unauthorized", {});
                return;
            }
            const std::string uri = request.getURI();
            if (uri.find("/list") == 0) {
                handleList(response, request);
            } else if (uri.find("/upload") == 0) {
                handleUpload(response, request);
            } else if (uri.find("/download") == 0) {
                handleDownload(response, request);
            } else {
                sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "Endpoint not found", {});
            }
        } catch (const std::exception &ex) {
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.what(), {});
        }
    }

private:
    bool authenticate(Poco::Net::HTTPServerRequest &request) {
        try {
            Poco::Net::HTTPBasicCredentials credentials(request);
            auto it = USERS.find(credentials.getUsername());
            return it != USERS.end() && it->second.first == credentials.getPassword();
        } catch (...) {
            return false;
        }
    }

    boost::filesystem::path resolvePath(const std::string &directoryPath) {
        return boost::filesystem::absolute(boost::filesystem::path(ROOT_DIRECTORY) / directoryPath);
    }

    bool validatePath(const boost::filesystem::path &resolvedPath, Poco::Net::HTTPServerResponse &response) {
        std::string rootPath = boost::filesystem::canonical(ROOT_DIRECTORY).generic_string();
        std::string resolvedPathStr = resolvedPath.generic_string();
        if (resolvedPathStr.find(rootPath) != 0) {
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_FORBIDDEN, "Access denied", {});
            return false;
        }
        return true;
    }

    void handleList(Poco::Net::HTTPServerResponse &response, Poco::Net::HTTPServerRequest &request) {
        Poco::Net::HTMLForm form(request);
        std::string directoryPath = form.has("path") ? form.get("path") : "./";
        try {
            boost::filesystem::path resolvedPath = resolvePath(directoryPath);
            if (!validatePath(resolvedPath, response)) return;
            if (!boost::filesystem::exists(resolvedPath) || !boost::filesystem::is_directory(resolvedPath)) {
                sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "Directory not found", {});
                return;
            }
            std::vector<std::string> files;
            for (const auto &entry : boost::filesystem::directory_iterator(resolvedPath)) {
                files.push_back(entry.path().filename().string());
            }
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_OK, "Directory listing successful", files);
        } catch (const std::exception &ex) {
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.what(), {});
        }
    }

    void handleUpload(Poco::Net::HTTPServerResponse &response, Poco::Net::HTTPServerRequest &request) {
        try {
            FilePartHandler partHandler(ROOT_DIRECTORY);
            Poco::Net::HTMLForm form(request, request.stream(), partHandler);
            if (partHandler.filePath().empty()) {
                sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "No file uploaded", {});
                return;
            }
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_OK, "File uploaded successfully", {});
        } catch (const std::exception &ex) {
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.what(), {});
        }
    }

    void handleDownload(Poco::Net::HTTPServerResponse &response, Poco::Net::HTTPServerRequest &request) {
        Poco::Net::HTMLForm form(request);
        if (!form.has("filename")) {
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Missing filename parameter", {});
            return;
        }
        std::string filename = form.get("filename");
        boost::filesystem::path filepath = resolvePath(filename);
        try {
            if (!validatePath(filepath, response)) return;
            if (!boost::filesystem::exists(filepath)) {
                sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_NOT_FOUND, "File not found", {});
                return;
            }
            response.setContentType("application/octet-stream");
            response.set("Content-Disposition", "attachment; filename=" + filename);
            std::ifstream infile(filepath.string(), std::ios::binary);
            response.send() << infile.rdbuf();
        } catch (const std::exception &ex) {
            sendJsonResponse(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR, ex.what(), {});
        }
    }

    void sendJsonResponse(Poco::Net::HTTPServerResponse &response, Poco::Net::HTTPResponse::HTTPStatus status, const std::string &message, const std::vector<std::string> &data) {
        response.setContentType("application/json");
        response.setStatus(status);
        std::ostringstream json;
        json << "{ \"status\": " << static_cast<int>(status) << ", \"message\": \"" << message << "\", \"data\": [";
        for (size_t i = 0; i < data.size(); ++i) {
            if (i > 0) json << ", ";
            json << "\"" << data[i] << "\"";
        }
        json << "] }";
        response.send() << json.str();
    }
};

class FileServerRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory {
public:
    Poco::Net::HTTPRequestHandler *createRequestHandler(const Poco::Net::HTTPServerRequest &) override {
        return new FileServerRequestHandler();
    }
};

int main() {
    try {
        Poco::Net::HTTPServerParams *params = new Poco::Net::HTTPServerParams();
        params->setMaxQueued(100);
        params->setMaxThreads(4);

        Poco::Net::ServerSocket socket(8080);
        Poco::Net::HTTPServer server(new FileServerRequestHandlerFactory(), socket, params);
        server.start();
        std::cout << "File Server running on port 8080. Nginx is handling SSL." << std::endl;
        std::cin.get();
        server.stop();
    } catch (const Poco::Exception &ex) {
        std::cerr << "Poco Exception: " << ex.displayText() << std::endl;
    } catch (const std::exception &ex) {
        std::cerr << "Standard Exception: " << ex.what() << std::endl;
    } catch (...) {
        std::cerr << "Unknown exception occurred." << std::endl;
    }
    return 0;
}
