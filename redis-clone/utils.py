from collections import namedtuple
from io import BytesIO

class CommandError(Exception): pass
class Disconnect(Exception): pass
Error = namedtuple('Error', ('message',))

class ProtocolHandler:
  def __init__(self):
    self.handlers = {
      '+': self.handle_simple_string,
      '-': self.handle_error,
      ':': self.handle_integer,
      '$': self.handle_string,
      '*': self.handle_array,
      '%': self.handle_dict
    }

  def handle_request(self, socket_file):
    first_byte = socket_file.read(1)
    print(first_byte)
    if not first_byte:
      raise Disconnect()
    
    try:
      # Delegate to the appropriate handler based on the first byte
      return self.handlers[first_byte](socket_file)
    except KeyError:
      raise CommandError('bad request')

  def parse_socket_file(self, socket_file):
    return socket_file.readline().rstrip('\r\n')

  def handle_simple_string(self, socket_file):
    return self.parse_socket_file(socket_file)

  def handle_error(self, socket_file):
    return Error(self.parse_socket_file(socket_file))

  def handle_integer(self, socket_file):
    return int(self.parse_socket_file(socket_file))

  def handle_string(self, socket_file):
    # First read the length ($<length>\r\n)
    length = int(self.parse_socket_file(socket_file))
    if length == -1:
      return None # Special casa for NULLs
    length += 2 # Include the trailing \r\n in count
    return socket_file.read(length)[:-2]

  def handle_array(self, socket_file):
    num_elements = int(self.parse_socket_file(socket_file))
    return [self.handle_request(socket_file) for _ in range(num_elements)]
  
  def handle_dict(self, socket_file):
    num_items = int(self.parse_socket_file(socket_file))
    elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
    return dict(zip(elements[::2], elements[1::2]))


  def write_response(self, socket_file, data):
    # Serialize the response data and sent it to the client.
    buf = BytesIO()
    self._write(buf, data)
    buf.seek(0)
    socket_file.write(buf.getvalue())
    socket_file.flush()

  def _write(self, buf, data):
    if isinstance(data, str):
      data = data.encode('utf-8')

    if isinstance(data, bytes):
      buf.write(f'${len(data)}\r\n{data}\r\n'.encode())
    elif isinstance(data, int):
      buf.write(f':{data}\r\n'.encode())
    elif isinstance(data, Error):
      buf.write(f'-{Error.message}\r\n'.encode())
    elif isinstance(data, (list, tuple)):
      buf.write(f'*{len(data)}\r\n'.encode())
      for item in data:
        self._write(buf, item)
    elif isinstance(data, dict):
      buf.write(f'%{len(data)}\r\n'.encode())
      for key in data:
        self._write(buf, key)
        self._write(buf, data[key])
    elif data is None:
      buf.write(f'$-1\r\n'.encode())
    else:
      raise CommandError(f'unrecognized type: {type(data)}')