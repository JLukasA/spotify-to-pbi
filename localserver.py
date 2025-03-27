from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse


class RedirectHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Extract the authorization code from the query parameters
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        code = params.get("code", [None])[0]

        if code:
            # Send a response to the browser
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"Authorization successful! You can close this tab.")
            # Store the code in a global variable or pass it to the main script
            self.server.authorization_code = code
        else:
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"Authorization failed. No code found.")


def run_server(server_address):
    httpd = HTTPServer(server_address, RedirectHandler)
    print(f"Server running at {server_address}")
    httpd.handle_request()  # Handle one request and then stop
    return httpd.authorization_code
