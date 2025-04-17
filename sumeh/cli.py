import os
import webbrowser
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer


def serve_index():
    """
    Serves the index.html file for initial configuration and opens it in a browser.

    This function determines the path to the 'index.html' file, changes the
    working directory to the appropriate location, and starts a simple HTTP server
    to serve the file. It also automatically opens the served page in the default
    web browser.

    The server runs on localhost at port 8000. The process continues until
    interrupted by the user (via a KeyboardInterrupt), at which point the server
    shuts down.

    Raises:
        KeyboardInterrupt: If the server is manually interrupted by the user.
    """  # Determine the directory of the index.html file
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
