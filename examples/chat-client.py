import socket
import struct
import threading

# def hexdump(buf:bytes):
#     line = []
#     for _,b in enumerate(buf):
#         line.append(f"{b:02x}")
#     return line

MAGIC_NUM = 0x7368

def send_msg(sock: socket.socket, msg_id, data):
    payload = data.encode('utf-8')
    body_len = len(payload) + 8
    header = struct.pack('>HHI', MAGIC_NUM, msg_id, body_len)
    session_id = struct.pack('>Q', 0)
    sock.sendall(header + session_id + payload)

def recv_loop(sock: socket.socket):
    HEADER_SIZE = struct.calcsize(">HHI")
    try:
        while True:
            header_buf = sock.recv(HEADER_SIZE)
            if not header_buf:
                print("\n[socket] Disconnect")
                break
            if len(header_buf) < HEADER_SIZE:
                continue

            _magic_num, msg_id, body_len = struct.unpack(">HHI", header_buf)
            if _magic_num != MAGIC_NUM:
                print("\nBad magic num:", _magic_num)

            body_buf = b""
            need = body_len
            while len(body_buf) < need:
                chunk = sock.recv(need - len(body_buf))
                if not chunk:
                    return
                body_buf += chunk


            session_id, = struct.unpack(">Q", body_buf[:8])
            payload = body_buf[8:]
            text = payload.decode("utf-8", errors="replace")

            print(f"\n<<< Receive | msg_id={msg_id}, session={session_id}, payload={text}")
            print("> ", end="", flush=True)

    except OSError:
        pass

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.connect(('127.0.0.1', 9527))

    recv_thread = threading.Thread(target=recv_loop, args=(sock,), daemon=True)
    recv_thread.start()

    try:
        while True:
            line = input("> ")
            send_msg(sock, 1, line)

    except KeyboardInterrupt:
        print("\n[Ctrl+C] Closed")
    finally:
        sock.close()

if __name__ == "__main__":
    main()
