import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

void main() {
  late ServerSocket server;
  late Completer<Uint8List> firstMessage;
  late Completer<void> serverSideClosed;

  setUp(() async {
    firstMessage = Completer<Uint8List>();
    serverSideClosed = Completer<void>();
    server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((socket) {
      final received = BytesBuilder();
      socket.listen(
        (data) {
          received.add(data);
          if (received.length >= 8 && !firstMessage.isCompleted) {
            firstMessage.complete(received.toBytes().sublist(0, 8));
            socket.destroy();
          }
        },
        // A reset can deliver an error and then done.
        onDone: () {
          if (!serverSideClosed.isCompleted) serverSideClosed.complete();
        },
        onError: (_) {
          if (!serverSideClosed.isCompleted) serverSideClosed.complete();
        },
      );
    });
  });
  tearDown(() => server.close());

  Future<Connection> open(SslMode sslMode, TlsConnector connector) =>
      Connection.open(
        Endpoint(
          host: 'localhost',
          port: server.port,
          database: 'dart_test',
          username: 'postgres',
        ),
        settings: ConnectionSettings(
          sslMode: sslMode,
          tlsConnector: connector,
          connectTimeout: Duration(milliseconds: 500),
        ),
      );

  test('tlsConnector replaces the SSLRequest', () async {
    SslMode? requested;
    Future<Socket> connector(
      String host,
      int port, {
      required SslMode sslMode,
      required Duration timeout,
    }) {
      requested = sslMode;
      return Socket.connect(host, port);
    }

    await expectLater(open(SslMode.verifyFull, connector), throwsA(anything));

    // A StartupMessage, protocol 3.0, where the SSLRequest code would be.
    expect((await firstMessage.future).sublist(4), [0, 3, 0, 0]);
    expect(requested, SslMode.verifyFull);
  });

  test('tlsConnector is not used with SslMode.disable', () async {
    var called = false;
    Future<Socket> connector(
      String host,
      int port, {
      required SslMode sslMode,
      required Duration timeout,
    }) {
      called = true;
      return Socket.connect(host, port);
    }

    await expectLater(open(SslMode.disable, connector), throwsA(anything));

    expect((await firstMessage.future).sublist(4), [0, 3, 0, 0]);
    expect(called, isFalse);
  });

  test('tlsConnector timeout does not leak the late socket', () async {
    Future<Socket> connector(
      String host,
      int port, {
      required SslMode sslMode,
      required Duration timeout,
    }) async {
      final socket = await Socket.connect(host, port);
      await Future<void>.delayed(timeout * 2);
      return socket;
    }

    await expectLater(
      open(SslMode.require, connector),
      throwsA(isA<TimeoutException>()),
    );

    await serverSideClosed.future.timeout(Duration(seconds: 5));
    expect(firstMessage.isCompleted, isFalse);
  });
}
