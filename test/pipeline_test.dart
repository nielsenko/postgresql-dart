import 'dart:async';

import 'package:async/async.dart' as async;
import 'package:postgres/postgres.dart';
import 'package:postgres/src/v3/protocol.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:test/test.dart';

import 'docker.dart';

/// With a pipeline depth above 1, statements started without awaiting the
/// previous one go out before the first is answered, and the answers come
/// back in order.
void main() {
  withPostgresServer('pipeline', (server) {
    /// Opens a connection that records each exchange it sends as 'sent' and
    /// each ReadyForQuery it receives as 'answered'.
    Future<(Connection, List<String>)> open(int pipelineDepth) async {
      final events = <String>[];
      final connection = await Connection.open(
        await server.endpoint(),
        settings: ConnectionSettings(
          pipelineDepth: pipelineDepth,
          connectTimeout: const Duration(seconds: 3),
          transformer: StreamChannelTransformer<Message, Message>(
            StreamTransformer.fromHandlers(
              handleData: (message, sink) {
                if (message is ReadyForQueryMessage) events.add('answered');
                sink.add(message);
              },
            ),
            async.StreamSinkTransformer.fromHandlers(
              handleData: (message, sink) {
                final messages = message is AggregatedClientMessage
                    ? message.messages
                    : [message];
                if (messages.any(
                  (m) => m is SyncMessage || m is QueryMessage,
                )) {
                  events.add('sent');
                }
                sink.add(message);
              },
            ),
          ),
        ),
      );
      // Connecting is not what is under test.
      events.clear();
      addTearDown(connection.close);
      return (connection, events);
    }

    Future<Object?> value(Connection connection, int i) async =>
        (await connection.execute(
          r'SELECT $1::int',
          parameters: [i],
        )).single.single;

    test(
      'statements started together go out before the first is answered',
      () async {
        final (connection, events) = await open(3);

        final values = await Future.wait([
          for (var i = 1; i <= 3; i++) value(connection, i),
        ]);

        expect(values, [1, 2, 3]);
        expect(events, [
          'sent',
          'sent',
          'sent',
          'answered',
          'answered',
          'answered',
        ]);
      },
    );

    test('a depth of 1 waits for each answer before sending more', () async {
      final (connection, events) = await open(1);

      final values = await Future.wait([
        for (var i = 1; i <= 3; i++) value(connection, i),
      ]);

      expect(values, [1, 2, 3]);
      expect(events, [
        'sent',
        'answered',
        'sent',
        'answered',
        'sent',
        'answered',
      ]);
    });

    test('no more statements than the depth are in flight', () async {
      final (connection, events) = await open(2);

      final values = await Future.wait([
        for (var i = 1; i <= 6; i++) value(connection, i),
      ]);

      expect(values, [1, 2, 3, 4, 5, 6]);
      var inFlight = 0;
      var most = 0;
      for (final event in events) {
        inFlight += event == 'sent' ? 1 : -1;
        if (inFlight > most) most = inFlight;
      }
      expect(most, 2);
    });

    test('outside a transaction, a failing statement fails alone', () async {
      final (connection, _) = await open(3);

      final results = await Future.wait([
        async.Result.capture(connection.execute('SELECT 1')),
        async.Result.capture(connection.execute('SELECT 1/0')),
        async.Result.capture(connection.execute('SELECT 3')),
      ]);

      expect(results[0].asValue!.value, [
        [1],
      ]);
      expect(
        results[1].asError!.error,
        isA<ServerException>().having((e) => e.code, 'code', '22012'),
      );
      expect(results[2].asValue!.value, [
        [3],
      ]);
    });

    test('the statements of a transaction go out together', () async {
      final (connection, events) = await open(4);
      await connection.execute('CREATE TEMPORARY TABLE t (id int)');
      events.clear();

      await connection.runTx((tx) async {
        await Future.wait([
          for (var i = 1; i <= 3; i++)
            tx.execute(r'INSERT INTO t VALUES ($1)', parameters: [i]),
        ]);
      });
      final transaction = [...events];

      expect(await connection.execute('SELECT count(*) FROM t'), [
        [3],
      ]);
      expect(transaction, [
        'sent', 'answered', // BEGIN
        'sent', 'sent', 'sent', // the inserts
        'answered', 'answered', 'answered',
        'sent', 'answered', // COMMIT
      ]);
    });

    test('in a transaction, the statements after a failing one fail, and '
        'runTx rolls back', () async {
      final (connection, _) = await open(4);
      await connection.execute('CREATE TEMPORARY TABLE t (id int)');

      final results = <async.Result<Object?>>[];
      await expectLater(
        connection.runTx((tx) async {
          results.addAll(
            await Future.wait([
              async.Result.capture(tx.execute('INSERT INTO t VALUES (1)')),
              async.Result.capture(tx.execute("INSERT INTO t VALUES ('x')")),
              async.Result.capture(tx.execute('INSERT INTO t VALUES (3)')),
            ]),
          );
        }),
        throwsA(isA<ServerException>()),
      );

      expect(results[0].isValue, isTrue);
      expect(
        results[1].asError!.error,
        isA<ServerException>().having((e) => e.code, 'code', '22P02'),
      );
      expect(
        results[2].asError!.error,
        isA<ServerException>().having((e) => e.code, 'code', '25P02'),
      );
      expect(await connection.execute('SELECT count(*) FROM t'), [
        [0],
      ]);
    });

    test('a timeout cancels its statement, and the statement sent after it '
        'still runs', () async {
      final (connection, _) = await open(2);

      final results = await Future.wait([
        async.Result.capture(
          connection.execute(
            'SELECT pg_sleep(2)',
            timeout: const Duration(milliseconds: 100),
          ),
        ),
        async.Result.capture(connection.execute('SELECT 2')),
      ]);

      expect(results[0].asError!.error, isA<TimeoutException>());
      expect(results[1].asValue!.value, [
        [2],
      ]);
    });

    test(
      'a timeout alone in flight is cancelled, and the connection goes on',
      () async {
        final (connection, _) = await open(2);

        await expectLater(
          connection.execute(
            'SELECT pg_sleep(2)',
            timeout: const Duration(milliseconds: 100),
          ),
          throwsA(isA<TimeoutException>()),
        );

        expect(await value(connection, 7), 7);
      },
    );

    test('close waits for the statements in flight', () async {
      final (connection, _) = await open(3);

      final pending = [
        for (var i = 1; i <= 3; i++)
          connection.execute(r'SELECT pg_sleep(0.1), $1::int', parameters: [i]),
      ];
      // Let them go out before closing.
      await Future<void>.delayed(const Duration(milliseconds: 20));
      await connection.close();

      final results = await Future.wait(pending);
      expect([for (final result in results) result.single[1]], [1, 2, 3]);
    });

    test('a forced close while a graceful close waits ends both', () async {
      final (connection, _) = await open(3);

      final pending = [
        for (var i = 1; i <= 3; i++)
          async.Result.capture(connection.execute('SELECT pg_sleep(10)')),
      ];
      // Let them go out before closing.
      await Future<void>.delayed(const Duration(milliseconds: 20));
      final graceful = connection.close();
      await connection.close(force: true);

      await graceful.timeout(const Duration(seconds: 2));
      for (final result in await Future.wait(pending)) {
        expect(result.asError!.error, isA<PgException>());
      }
    });
  });
}
