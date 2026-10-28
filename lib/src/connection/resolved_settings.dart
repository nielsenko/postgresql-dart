import 'dart:convert';
import 'dart:io';

import 'package:postgres/messages.dart';
import 'package:stack_trace/stack_trace.dart';
import 'package:stream_channel/stream_channel.dart';

import '../../postgres.dart';

Duration _requirePositive(String name, Duration value) {
  if (value <= Duration.zero) {
    throw ArgumentError('$name must be positive, got $value.');
  }
  return value;
}

KeepAliveSettings _resolveKeepAlive(
  KeepAliveSettings? settings,
  KeepAliveSettings? fallback,
) {
  final idle = settings?.idle ?? fallback?.idle;
  final interval = settings?.interval ?? fallback?.interval;
  final count = settings?.count ?? fallback?.count;
  if (idle != null) _requirePositive('keepAlive.idle', idle);
  if (interval != null) _requirePositive('keepAlive.interval', interval);
  if (count != null && count <= 0) {
    throw ArgumentError('keepAlive.count must be positive, got $count.');
  }
  return KeepAliveSettings(
    enabled: settings?.enabled ?? fallback?.enabled ?? true,
    idle: idle,
    interval: interval,
    count: count,
  );
}

class ResolvedSessionSettings implements SessionSettings {
  @override
  final Duration connectTimeout;
  @override
  final Duration queryTimeout;
  @override
  final QueryMode queryMode;
  @override
  final bool ignoreSuperfluousParameters;
  @override
  final bool ignoreStackTraces;

  ResolvedSessionSettings(SessionSettings? settings, SessionSettings? fallback)
    : connectTimeout = _requirePositive(
        'connectTimeout',
        settings?.connectTimeout ??
            fallback?.connectTimeout ??
            Duration(seconds: 15),
      ),
      queryTimeout = _requirePositive(
        'queryTimeout',
        settings?.queryTimeout ??
            fallback?.queryTimeout ??
            Duration(minutes: 5),
      ),
      queryMode =
          settings?.queryMode ?? fallback?.queryMode ?? QueryMode.extended,
      ignoreSuperfluousParameters =
          settings?.ignoreSuperfluousParameters ??
          fallback?.ignoreSuperfluousParameters ??
          false,
      ignoreStackTraces =
          settings?.ignoreStackTraces ?? fallback?.ignoreStackTraces ?? false;

  bool isMatchingSession(ResolvedSessionSettings other) {
    return connectTimeout == other.connectTimeout &&
        queryTimeout == other.queryTimeout &&
        queryMode == other.queryMode &&
        ignoreSuperfluousParameters == other.ignoreSuperfluousParameters &&
        ignoreStackTraces == other.ignoreStackTraces;
  }

  /// `StackTrace.current`, or `StackTrace.empty` if [ignoreStackTraces].
  StackTrace captureStackTrace() =>
      ignoreStackTraces ? StackTrace.empty : StackTrace.current;

  /// `Trace.current()`, or an empty [Trace] if [ignoreStackTraces].
  Trace captureTrace() =>
      ignoreStackTraces ? Trace.from(StackTrace.empty) : Trace.current();
}

class ResolvedConnectionSettings extends ResolvedSessionSettings
    implements ConnectionSettings {
  @override
  final String? applicationName;
  @override
  final String timeZone;
  @override
  final Encoding encoding;
  @override
  final SslMode sslMode;
  @override
  final SecurityContext? securityContext;
  @override
  final StreamChannelTransformer<Message, Message>? transformer;
  @override
  final ReplicationMode replicationMode;
  @override
  final TypeRegistry typeRegistry;
  @override
  final Future<void> Function(Connection connection)? onOpen;
  @override
  final KeepAliveSettings keepAlive;
  @override
  final TlsConnector? tlsConnector;

  ResolvedConnectionSettings(
    ConnectionSettings? super.settings,
    ConnectionSettings? super.fallback,
  ) : applicationName = settings?.applicationName ?? fallback?.applicationName,
      timeZone = settings?.timeZone ?? fallback?.timeZone ?? 'UTC',
      encoding = settings?.encoding ?? fallback?.encoding ?? utf8,
      sslMode = settings?.sslMode ?? fallback?.sslMode ?? SslMode.require,
      securityContext = _tlsSource(settings, fallback)?.securityContext,
      tlsConnector = _tlsSource(settings, fallback)?.tlsConnector,
      // TODO: consider merging the transformers
      transformer = settings?.transformer ?? fallback?.transformer,
      replicationMode =
          settings?.replicationMode ??
          fallback?.replicationMode ??
          ReplicationMode.none,
      // TODO: consider merging the type registries
      typeRegistry =
          settings?.typeRegistry ?? fallback?.typeRegistry ?? TypeRegistry(),
      onOpen = settings?.onOpen ?? fallback?.onOpen,
      keepAlive = _resolveKeepAlive(settings?.keepAlive, fallback?.keepAlive) {
    if (securityContext != null && tlsConnector != null) {
      throw ArgumentError(
        'securityContext does not apply to connections that tlsConnector '
        'opens. Set only one of them.',
      );
    }
  }

  /// The settings that both [securityContext] and [tlsConnector] come from.
  ///
  /// They resolve as a pair because the connector replaces the TLS that the
  /// context configures. Resolved one by one, a context from [settings] would
  /// go unused under a connector from [fallback].
  static ConnectionSettings? _tlsSource(
    ConnectionSettings? settings,
    ConnectionSettings? fallback,
  ) => settings?.securityContext != null || settings?.tlsConnector != null
      ? settings
      : fallback;

  bool isMatchingConnection(ResolvedConnectionSettings other) {
    // `onOpen` isn't compared. It only runs when a connection is first
    // opened (see its doc comment), so it doesn't affect whether an open
    // connection is safe to reuse. Comparing it would let a fresh closure
    // per call, a natural Dart pattern, silently defeat pooling.
    // `tlsConnector` is compared, because it decides how a connection is
    // secured and also opens the side connection that cancels a statement.
    return isMatchingSession(other) &&
        applicationName == other.applicationName &&
        timeZone == other.timeZone &&
        encoding == other.encoding &&
        sslMode == other.sslMode &&
        securityContext == other.securityContext &&
        tlsConnector == other.tlsConnector &&
        transformer == other.transformer &&
        replicationMode == other.replicationMode &&
        typeRegistry == other.typeRegistry &&
        keepAlive == other.keepAlive;
  }
}

class ResolvedPoolSettings extends ResolvedConnectionSettings
    implements PoolSettings {
  @override
  final int maxConnectionCount;
  @override
  final Duration maxConnectionAge;
  @override
  final Duration maxSessionUse;
  @override
  final int maxQueryCount;
  @override
  final bool resetSession;
  @override
  final Duration idleCheckAfter;

  ResolvedPoolSettings(PoolSettings? settings)
    : maxConnectionCount = settings?.maxConnectionCount ?? 1,
      maxConnectionAge = settings?.maxConnectionAge ?? Duration(hours: 12),
      maxSessionUse = settings?.maxSessionUse ?? Duration(hours: 4),
      maxQueryCount = settings?.maxQueryCount ?? 100000,
      resetSession = settings?.resetSession ?? true,
      idleCheckAfter = settings?.idleCheckAfter ?? Duration.zero,
      super(settings, null);
}

class ResolvedTransactionSettings extends ResolvedSessionSettings
    implements TransactionSettings {
  @override
  final IsolationLevel? isolationLevel;
  @override
  final AccessMode? accessMode;
  @override
  final DeferrableMode? deferrable;

  ResolvedTransactionSettings(
    TransactionSettings? super.settings,
    super.fallback,
  ) : isolationLevel = settings?.isolationLevel,
      accessMode = settings?.accessMode,
      deferrable = settings?.deferrable;
}
