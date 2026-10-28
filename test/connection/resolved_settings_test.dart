import 'dart:io';

import 'package:postgres/postgres.dart';
import 'package:postgres/src/connection/resolved_settings.dart';
import 'package:test/test.dart';

void main() {
  group('ResolvedConnectionSettings.isMatchingConnection', () {
    test('connections with different typeRegistry do not match', () {
      final a = ResolvedConnectionSettings(
        ConnectionSettings(typeRegistry: TypeRegistry()),
        null,
      );
      final b = ResolvedConnectionSettings(
        ConnectionSettings(typeRegistry: TypeRegistry()),
        null,
      );

      expect(a.isMatchingConnection(b), isFalse);
      expect(a.isMatchingConnection(a), isTrue);
    });

    test('connections with different securityContext do not match', () {
      final a = ResolvedConnectionSettings(
        ConnectionSettings(securityContext: SecurityContext()),
        null,
      );
      final b = ResolvedConnectionSettings(
        ConnectionSettings(securityContext: SecurityContext()),
        null,
      );

      expect(a.isMatchingConnection(b), isFalse);
      expect(a.isMatchingConnection(a), isTrue);
    });

    test('connections with different (but otherwise equal) onOpen closures '
        'still match', () {
      // A fresh closure per call is a natural Dart pattern - `onOpen` only
      // runs once, at connection creation, so it must not prevent reuse of
      // an already-open connection. `typeRegistry` is shared explicitly
      // since two default `TypeRegistry()` instances aren't `==` to each
      // other, which would otherwise mask what this test is checking.
      final typeRegistry = TypeRegistry();
      final a = ResolvedConnectionSettings(
        ConnectionSettings(onOpen: (c) async {}, typeRegistry: typeRegistry),
        null,
      );
      final b = ResolvedConnectionSettings(
        ConnectionSettings(onOpen: (c) async {}, typeRegistry: typeRegistry),
        null,
      );

      expect(a.isMatchingConnection(b), isTrue);
    });

    test('connections with different tlsConnector do not match', () {
      final typeRegistry = TypeRegistry();
      ResolvedConnectionSettings resolve(TlsConnector connector) =>
          ResolvedConnectionSettings(
            ConnectionSettings(
              tlsConnector: connector,
              typeRegistry: typeRegistry,
            ),
            null,
          );

      expect(
        resolve(
          _unusedConnector,
        ).isMatchingConnection(resolve(_otherUnusedConnector)),
        isFalse,
      );
      expect(
        resolve(
          _unusedConnector,
        ).isMatchingConnection(resolve(_unusedConnector)),
        isTrue,
      );
    });
  });

  group('ResolvedConnectionSettings TLS', () {
    test('securityContext replaces the tlsConnector of the fallback', () {
      final context = SecurityContext();

      final settings = ResolvedConnectionSettings(
        ConnectionSettings(securityContext: context),
        ConnectionSettings(tlsConnector: _unusedConnector),
      );

      expect(settings.securityContext, same(context));
      expect(settings.tlsConnector, isNull);
    });

    test('tlsConnector replaces the securityContext of the fallback', () {
      final settings = ResolvedConnectionSettings(
        ConnectionSettings(tlsConnector: _unusedConnector),
        ConnectionSettings(securityContext: SecurityContext()),
      );

      expect(settings.securityContext, isNull);
      expect(settings.tlsConnector, _unusedConnector);
    });

    test('settings without either keep those of the fallback', () {
      final settings = ResolvedConnectionSettings(
        ConnectionSettings(applicationName: 'app'),
        ConnectionSettings(tlsConnector: _unusedConnector),
      );

      expect(settings.tlsConnector, _unusedConnector);
    });

    test('settings that set both throw', () {
      final both = ConnectionSettings(
        securityContext: SecurityContext(),
        tlsConnector: _unusedConnector,
      );

      expect(() => ResolvedConnectionSettings(both, null), throwsArgumentError);
      expect(
        () => ResolvedPoolSettings(
          PoolSettings(
            securityContext: SecurityContext(),
            tlsConnector: _unusedConnector,
          ),
        ),
        throwsArgumentError,
      );
    });
  });

  group('ResolvedSessionSettings rejects non-positive timeouts', () {
    test('zero connectTimeout throws', () {
      expect(
        () => ResolvedSessionSettings(
          SessionSettings(connectTimeout: Duration.zero),
          null,
        ),
        throwsArgumentError,
      );
    });

    test('negative queryTimeout throws', () {
      expect(
        () => ResolvedSessionSettings(
          SessionSettings(queryTimeout: Duration(seconds: -1)),
          null,
        ),
        throwsArgumentError,
      );
    });

    test('positive timeouts are accepted', () {
      final settings = ResolvedSessionSettings(
        SessionSettings(
          connectTimeout: Duration(seconds: 1),
          queryTimeout: Duration(seconds: 1),
        ),
        null,
      );
      expect(settings.connectTimeout, Duration(seconds: 1));
      expect(settings.queryTimeout, Duration(seconds: 1));
    });
  });
}

Future<Socket> _unusedConnector(TlsConnectorContext context) =>
    throw UnimplementedError();

Future<Socket> _otherUnusedConnector(TlsConnectorContext context) =>
    throw UnimplementedError();
