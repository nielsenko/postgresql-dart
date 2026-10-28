import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:charcode/ascii.dart';
import 'package:meta/meta.dart';

import '../../postgres.dart';

/// Opens the connection for [ConnectionSettings.tlsConnector].
///
/// It returns a socket that carries the Postgres protocol in plain text, with
/// TLS underneath.
typedef TlsConnector = Future<Socket> Function(TlsConnectorContext context);

/// One connection attempt of a [TlsConnector].
///
/// The connector connects to [host] and [port] and completes a TLS handshake
/// that honors [sslMode]. A Postgres server first expects an SSLRequest,
/// which [requestSsl] sends. Postgres 17 and later also accept a handshake
/// right away, without one, if it offers the `postgresql` ALPN protocol.
///
/// ```dart
/// Future<Socket> connect(TlsConnectorContext context) async {
///   final socket = await Socket.connect(
///     context.host,
///     context.port,
///     timeout: context.timeout,
///   );
///   await context.requestSsl(socket);
///   return SecureSocket.secure(
///     socket,
///     host: context.host,
///     onBadCertificate: context.sslMode.ignoreCertificateIssues
///         ? (_) => true
///         : (c) => throw BadCertificateException(c),
///   );
/// }
/// ```
///
/// The package treats the socket like the ones it secures itself:
///
/// - It sends a clear text password only over a [SecureSocket]. A connector
///   that runs TLS natively can return its own [SecureSocket] implementation.
/// - It enables TCP_NODELAY and [ConnectionSettings.keepAlive] on a TCP
///   socket. A socket that can't set an option throws an [UnsupportedError],
///   which the package ignores.
/// - Errors that the connector throws reach the caller unchanged. The
///   built-in TLS throws a [BadCertificateException] for a rejected
///   certificate, and the example above does the same.
///
/// The package waits up to [timeout] for the socket. Then it throws a
/// [TimeoutException], completes [abandoned], and destroys a socket that the
/// connector returns later. The connector has to release anything else that
/// it still holds, such as a handshake in progress.
///
/// The connector also opens the side connection that cancels a running
/// statement.
final class TlsConnectorContext {
  final String host;
  final int port;

  /// The TLS mode to honor, which is never [SslMode.disable].
  final SslMode sslMode;

  /// How long the package waits for the socket.
  final Duration timeout;

  /// Completes when the package gives up waiting for the socket.
  ///
  /// It doesn't complete for an attempt that returns or fails within
  /// [timeout].
  final Future<void> abandoned;

  @internal
  TlsConnectorContext({
    required this.host,
    required this.port,
    required this.sslMode,
    required this.timeout,
    required this.abandoned,
  });

  /// Sends an SSLRequest over [socket] and waits for the server to accept it.
  ///
  /// It throws the [PgException] of the built-in TLS when the server refuses
  /// SSL, and a [TimeoutException] after [timeout]. On failure it destroys
  /// [socket].
  ///
  /// It reads the server's answer from [socket], so afterwards [socket] can
  /// only be upgraded, such as with [SecureSocket.secure].
  Future<void> requestSsl(Socket socket) => negotiateSsl(socket, timeout);
}

/// Sends an SSLRequest over [socket] and waits up to [timeout] for the
/// server to accept it.
///
/// It throws a [PgException] when the server refuses SSL or answers with
/// more than one byte. On failure it destroys [socket].
///
/// It listens to [socket] and leaves the subscription paused, so the only
/// use left for [socket] is an upgrade such as [SecureSocket.secure].
Future<void> negotiateSsl(Socket socket, Duration timeout) async {
  final answer = Completer<int>();
  // ignore: cancel_subscriptions
  final subscription = socket.listen(
    (data) {
      if (answer.isCompleted) {
        return;
      }
      if (data.length != 1) {
        answer.completeError(
          PgException(
            'Could not initialize SSL connection, received unknown byte stream.',
          ),
        );
        return;
      }
      answer.complete(data.first);
    },
    onDone: () {
      if (!answer.isCompleted) {
        answer.completeError(
          PgException(
            'Could not initialize SSL connection, connection closed during handshake.',
          ),
        );
      }
    },
    onError: (Object error, StackTrace stackTrace) {
      if (!answer.isCompleted) {
        answer.completeError(error, stackTrace);
      }
    },
  );
  try {
    socket.add(
      (ByteData(8)
            ..setUint32(0, 8)
            ..setUint32(4, 80877103))
          .buffer
          .asUint8List(),
    );
    if (await answer.future.timeout(timeout) != $S) {
      throw PgException(
        'Server does not support SSL, but it was required (default configuration). '
        'To disable secure connections, use `ConnectionSettings(sslMode: SslMode.disable)`.',
      );
    }
    subscription.pause();
  } catch (_) {
    socket.destroy();
    unawaited(subscription.cancel());
    rethrow;
  }
}
