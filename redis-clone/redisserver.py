from gevent.pool import Pool
from gevent.server import StreamServer
from utils import *
class Server:
  def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
    self._pool = Pool(max_clients)
    self._server = StreamServer(
      (host, port),
      self.connection_handler,
      spawn=self._pool
    )

    self._protocol = ProtocolHandler()
    self._kv = {}

  def get_commands(self):
    return {
      'GET': self.get,
      'SET': self.set,
      'DELETE': self.delete,
      'FLUSH': self.flush,      
      'MGET': self.mget,
      'MSET': self.mset
    }

  def get(self, key):
    return self._kv.get(key)

  def set(self, key, value):
    self._kv[key] = value
    return 1

  def delete(self, key):
    if key in self._kv:
      del self._kv[key]
      return 1
    return 0

  def flush(self):
    kvlen = len(self._kv)
    self._kv.clear()
    return kvlen

  def mget(self, *keys):
    return [self._kv.get(key) for key in keys]

  def mset(self, *items):
    data = zip(items[::2], items[1::2])
    for key, value in data:
      self._kv[key] = value
    return len(data)

  def connection_handler(self, conn, address):
    # Convert "conn" (a socket object) into a file-like object
    socket_file = conn.makefile('rwb')

    # Process client requests until client disconnects
    while True:
      try:
        data = self._protocol.handle_request(socket_file)
      except Disconnect:
        break

      try:
        resp = self.get_response(data)
      except CommandError as exc:
        resp = Error(exc.args[0])

      self._protocol.write_response(socket_file, resp)

  def get_response(self, data):
    # Here we'll unpack the data sent by the client, execute the command and pass back the value
    if not isinstance(data, list):
      try:
        data = data.split()
      except:
        raise CommandError('Request must be list or simple string')
      
    if not data:
      raise CommandError(f'Missing command')

    command = data[0].upper()
    if command not in self._commands:
      raise CommandError(f'Unrecognized command: {command}')

    return self._commands[command](*data[1:])

  def run(self):
    self._server.serve_forever()

  
if __name__ == '__main__':
  from gevent import monkey; monkey.patch_all()
  Server().run()