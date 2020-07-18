import socket

class TCPConnector:

    def connect_to_socket(self, tcp_ip = "localhost", tcp_port: int = 9009):
        conn = None 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((tcp_ip, tcp_port))
        s.listen(1)
        print("Waiting for TCP connection...")
        conn, addr = s.accept()
        print("Connected... Starting getting tweets.")
        return conn
