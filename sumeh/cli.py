import os
import webbrowser
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer

def serve_index():
    """Serve the index.html file for initial configuration and open it in a browser."""
    # Determine the directory of the index.html file
    base_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(base_dir, "services")

    # Change the current working directory to the location of index.html
    os.chdir(html_path)

    # Define the port and URL
    port = 8000
    url = f"http://localhost:{port}"

    # Serve the HTML file
    with TCPServer(("localhost", port), SimpleHTTPRequestHandler) as httpd:
        print(f"Serving index.html at {url}")

        # Open the URL in the default web browser
        webbrowser.open(url)

        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server.")
            httpd.server_close()
