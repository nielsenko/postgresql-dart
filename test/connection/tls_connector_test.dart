import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:charcode/ascii.dart';
import 'package:postgres/postgres.dart';
import 'package:postgres/src/connection/keep_alive.dart';
import 'package:test/test.dart';

import '../support/docker.dart';

/// TCP_NODELAY has this number on Linux, macOS and Windows.
const _tcpNoDelay = 1;

void main() {
  late List<TlsConnectorContext> requests;
  late List<Socket> sockets;

  setUp(() {
    requests = [];
    sockets = [];
  });

  /// Connects without TLS, so that a fake server can read what follows.
  Future<Socket> connector(TlsConnectorContext context) async {
    requests.add(context);
    final socket = await Socket.connect(context.host, context.port);
    sockets.add(socket);
    return socket;
  }

  Endpoint endpointOf(_FakeServer server, {String? password}) => Endpoint(
    host: '127.0.0.1',
    port: server.port,
    database: 'dart_test',
    username: 'postgres',
    password: password,
  );

  Future<Connection> open(
    _FakeServer server, {
    SslMode sslMode = SslMode.verifyFull,
    TlsConnector? tlsConnector,
    KeepAliveSettings? keepAlive,
    Duration connectTimeout = const Duration(seconds: 5),
    String? password,
  }) => Connection.open(
    endpointOf(server, password: password),
    settings: ConnectionSettings(
      sslMode: sslMode,
      tlsConnector: tlsConnector ?? connector,
      keepAlive: keepAlive,
      connectTimeout: connectTimeout,
    ),
  );

  group('tlsConnector', () {
    test('opens the connection in place of the SSLRequest', () async {
      final server = await _FakeServer.bind();

      final connection = await open(server);
      addTearDown(connection.close);

      expect(requests.single.host, '127.0.0.1');
      expect(requests.single.port, server.port);
      expect(requests.single.sslMode, SslMode.verifyFull);
      expect(server.connections.single.messages, ['startup']);
    });

    test('reads the server through the socket it returns', () async {
      final server = await _FakeServer.bind(
        startupReply: _errorResponse('go away'),
      );

      await expectLater(
        open(server),
        throwsA(
          isA<ServerException>().having((e) => e.message, 'message', 'go away'),
        ),
      );
    });

    test('is not used with SslMode.disable', () async {
      final server = await _FakeServer.bind();

      final connection = await open(server, sslMode: SslMode.disable);
      addTearDown(connection.close);

      expect(requests, isEmpty);
      expect(server.connections.single.messages, ['startup']);
    });

    test('is not used for a Unix socket', () async {
      final directory = await Directory.systemTemp.createTemp('pg-tls-');
      addTearDown(() => directory.delete(recursive: true));
      final path = '${directory.path}/socket';
      final server = await _FakeServer.bind(
        address: InternetAddress(path, type: InternetAddressType.unix),
      );

      await expectLater(
        Connection.open(
          Endpoint(
            host: path,
            port: 0,
            database: 'dart_test',
            isUnixSocket: true,
          ),
          settings: ConnectionSettings(
            sslMode: SslMode.require,
            tlsConnector: connector,
          ),
        ),
        throwsA(isA<PgException>()),
      );

      expect(requests, isEmpty);
      expect(server.connections.single.messages, ['ssl']);
    }, skip: Platform.isWindows ? 'Unix sockets' : false);

    test('comes from the pool settings', () async {
      final server = await _FakeServer.bind();
      final pool = Pool.withEndpoints(
        [endpointOf(server)],
        settings: PoolSettings(
          sslMode: SslMode.verifyFull,
          tlsConnector: connector,
        ),
      );
      addTearDown(pool.close);

      await pool.withConnection((_) async {});

      expect(requests, hasLength(1));
    });

    test('a pool reuses a connection only for the same connector', () async {
      final server = await _FakeServer.bind();
      final otherRequests = <TlsConnectorContext>[];
      Future<Socket> otherConnector(TlsConnectorContext context) {
        otherRequests.add(context);
        return Socket.connect(context.host, context.port);
      }

      final pool = Pool.withEndpoints(
        [endpointOf(server)],
        settings: PoolSettings(
          sslMode: SslMode.verifyFull,
          tlsConnector: connector,
          maxConnectionCount: 2,
        ),
      );
      addTearDown(pool.close);

      await pool.withConnection((_) async {});
      await pool.withConnection(
        (_) async {},
        settings: ConnectionSettings(tlsConnector: otherConnector),
      );
      await pool.withConnection((_) async {});

      expect(requests, hasLength(1));
      expect(otherRequests, hasLength(1));
      expect(server.connections, hasLength(2));
    });

    test('gets TCP_NODELAY and keep-alive on its socket', () async {
      final options = KeepAliveOptions.current!;
      final server = await _FakeServer.bind();

      final connection = await open(
        server,
        keepAlive: KeepAliveSettings(idle: Duration(seconds: 61)),
      );
      addTearDown(connection.close);

      int? read(int level, int option) =>
          readRawIntOption(sockets.single, level, option);
      expect(read(RawSocketOption.levelTcp, _tcpNoDelay), isNot(0));
      expect(read(RawSocketOption.levelSocket, options.soKeepAlive), isNot(0));
      expect(read(RawSocketOption.levelTcp, options.idle), 61);
    }, skip: KeepAliveOptions.current == null ? 'unsupported platform' : false);

    test('may return a Unix socket', () async {
      final directory = await Directory.systemTemp.createTemp('pg-tls-');
      addTearDown(() => directory.delete(recursive: true));
      final address = InternetAddress(
        '${directory.path}/socket',
        type: InternetAddressType.unix,
      );
      final unixServer = await _FakeServer.bind(address: address);
      final tcpServer = await _FakeServer.bind();

      final connection = await open(
        tcpServer,
        tlsConnector: (_) => Socket.connect(address, 0),
      );
      addTearDown(connection.close);

      expect(unixServer.connections.single.messages, ['startup']);
    }, skip: Platform.isWindows ? 'Unix sockets' : false);

    test('a clear text password needs a SecureSocket', () async {
      final server = await _FakeServer.bind(
        startupReply: _cleartextPasswordRequest,
      );

      await expectLater(
        open(server, password: 'secret'),
        throwsA(
          isA<PgException>().having(
            (e) => e.message,
            'message',
            'Refused to send clear text password to server',
          ),
        ),
      );

      final connection = server.connections.single;
      await connection.closed.timeout(Duration(seconds: 5));
      expect(connection.messages, isNot(contains('p')));
    });

    test('requestSsl fails like the built-in TLS when refused', () async {
      final server = await _FakeServer.bind();
      Future<Socket> negotiating(TlsConnectorContext context) async {
        final socket = await Socket.connect(context.host, context.port);
        await context.requestSsl(socket);
        return socket;
      }

      final builtIn = await _errorOf(
        Connection.open(
          endpointOf(server),
          settings: ConnectionSettings(sslMode: SslMode.verifyFull),
        ),
      );
      final viaConnector = await _errorOf(
        open(server, tlsConnector: negotiating),
      );

      expect(viaConnector, isA<PgException>());
      expect(
        (viaConnector as PgException).message,
        (builtIn as PgException).message,
      );
      final connection = server.connections.last;
      await connection.closed.timeout(Duration(seconds: 5));
      expect(connection.messages, ['ssl']);
    });

    test('a socket it returns after the timeout is destroyed', () async {
      final server = await _FakeServer.bind();
      Future<Socket> lateConnector(TlsConnectorContext context) async {
        final socket = await Socket.connect(context.host, context.port);
        await context.abandoned;
        return socket;
      }

      await expectLater(
        open(
          server,
          tlsConnector: lateConnector,
          connectTimeout: Duration(milliseconds: 50),
        ),
        throwsA(isA<TimeoutException>()),
      );

      final connection = await server.firstConnection;
      await connection.closed.timeout(Duration(seconds: 5));
      expect(connection.messages, isEmpty);
    });
  });

  withPostgresServer('tlsConnector over TLS', (server) {
    Future<Socket> secureConnector(TlsConnectorContext context) async {
      requests.add(context);
      final socket = await Socket.connect(
        context.host,
        context.port,
        timeout: context.timeout,
      );
      await context.requestSsl(socket);
      final secure = await SecureSocket.secure(
        socket,
        host: context.host,
        onBadCertificate: context.sslMode.ignoreCertificateIssues
            ? (_) => true
            : (c) => throw BadCertificateException(c),
      );
      sockets.add(secure);
      return secure;
    }

    Future<Connection> openSecure({
      SslMode sslMode = SslMode.require,
      TlsConnector? tlsConnector,
      KeepAliveSettings? keepAlive,
      Duration? queryTimeout,
    }) async => Connection.open(
      await server.endpoint(),
      settings: ConnectionSettings(
        sslMode: sslMode,
        tlsConnector: tlsConnector,
        keepAlive: keepAlive,
        queryTimeout: queryTimeout,
      ),
    );

    test('connects with requestSsl and SecureSocket.secure', () async {
      final connection = await openSecure(tlsConnector: secureConnector);
      addTearDown(connection.close);

      final rows = await connection.execute(
        'SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()',
      );

      expect(rows, [
        [true],
      ]);
      expect(requests, hasLength(1));
    });

    test('sets TCP options through the SecureSocket', () async {
      final options = KeepAliveOptions.current!;
      final connection = await openSecure(
        tlsConnector: secureConnector,
        keepAlive: KeepAliveSettings(idle: Duration(seconds: 61)),
      );
      addTearDown(connection.close);

      int? read(int level, int option) =>
          readRawIntOption(sockets.single, level, option);
      expect(read(RawSocketOption.levelTcp, _tcpNoDelay), isNot(0));
      expect(read(RawSocketOption.levelSocket, options.soKeepAlive), isNot(0));
      expect(read(RawSocketOption.levelTcp, options.idle), 61);
    }, skip: KeepAliveOptions.current == null ? 'unsupported platform' : false);

    test('opens the connection that cancels a statement', () async {
      final connection = await openSecure(
        tlsConnector: secureConnector,
        queryTimeout: Duration(milliseconds: 500),
      );
      addTearDown(connection.close);

      await expectLater(
        connection.execute('SELECT pg_sleep(10)'),
        throwsA(isA<ServerException>().having((e) => e.code, 'code', '57014')),
      );

      expect(requests, hasLength(2));
    });

    test('rejects a certificate like the built-in TLS', () async {
      final builtIn = await _errorOf(openSecure(sslMode: SslMode.verifyFull));
      final viaConnector = await _errorOf(
        openSecure(sslMode: SslMode.verifyFull, tlsConnector: secureConnector),
      );

      expect(builtIn, isA<BadCertificateException>());
      expect(viaConnector, isA<BadCertificateException>());
    });
  });
}

/// The error that [future] fails with.
Future<Object> _errorOf(Future<void> future) => future.then<Object>(
  (_) => fail('Expected an error.'),
  onError: (Object error) => error,
);

/// AuthenticationOk, then ReadyForQuery for an idle connection.
const _ready = [82, 0, 0, 0, 8, 0, 0, 0, 0, 90, 0, 0, 0, 5, 73];

/// AuthenticationCleartextPassword.
const _cleartextPasswordRequest = [82, 0, 0, 0, 8, 0, 0, 0, 3];

/// An ErrorResponse that fails the connection with [message].
List<int> _errorResponse(String message) {
  final fields = [
    $S,
    ...ascii.encode('FATAL'),
    0,
    $M,
    ...ascii.encode(message),
    0,
    0,
  ];
  final length = ByteData(4)..setUint32(0, fields.length + 4);
  return [$E, ...length.buffer.asUint8List(), ...fields];
}

/// Stands in for a Postgres server and answers only what these tests need.
///
/// It refuses an SSLRequest and answers a StartupMessage with a fixed reply.
class _FakeServer {
  _FakeServer._(this._listener, this._startupReply) {
    _listener.listen(_serve);
  }

  static Future<_FakeServer> bind({
    InternetAddress? address,
    List<int> startupReply = _ready,
  }) async {
    final server = _FakeServer._(
      await ServerSocket.bind(address ?? InternetAddress.loopbackIPv4, 0),
      startupReply,
    );
    addTearDown(server._close);
    return server;
  }

  final ServerSocket _listener;
  final List<int> _startupReply;
  final _sockets = <Socket>[];
  final connections = <_FakeConnection>[];
  final _first = Completer<_FakeConnection>();

  int get port => _listener.port;

  /// The first connection that a client opened.
  Future<_FakeConnection> get firstConnection => _first.future;

  Future<void> _close() async {
    for (final socket in _sockets) {
      socket.destroy();
    }
    await _listener.close();
  }

  void _serve(Socket socket) {
    _sockets.add(socket);
    final connection = _FakeConnection();
    connections.add(connection);
    if (!_first.isCompleted) _first.complete(connection);

    var pending = Uint8List(0);
    var started = false;
    socket.listen(
      (data) {
        pending = Uint8List.fromList([...pending, ...data]);
        while (true) {
          final view = ByteData.sublistView(pending);
          if (!started) {
            // The length, then the protocol version or the SSLRequest code.
            if (pending.length < 8 || pending.length < view.getUint32(0)) {
              return;
            }
            final length = view.getUint32(0);
            if (view.getUint32(4) == 80877103) {
              connection.messages.add('ssl');
              socket.add([$N]);
            } else {
              connection.messages.add('startup');
              socket.add(_startupReply);
              started = true;
            }
            pending = pending.sublist(length);
          } else {
            // A type byte, then the length of the rest.
            if (pending.length < 5 || pending.length < 1 + view.getUint32(1)) {
              return;
            }
            connection.messages.add(String.fromCharCode(pending[0]));
            pending = pending.sublist(1 + view.getUint32(1));
          }
        }
      },
      onDone: connection._markClosed,
      // A reset can deliver an error and then done.
      onError: (Object _) => connection._markClosed(),
    );
  }
}

class _FakeConnection {
  /// What the client sent, in order: `ssl` for an SSLRequest, `startup` for a
  /// StartupMessage and the type of any later message.
  final messages = <String>[];
  final _closed = Completer<void>();

  /// Completes when the client closes the connection.
  Future<void> get closed => _closed.future;

  void _markClosed() {
    if (!_closed.isCompleted) _closed.complete();
  }
}
