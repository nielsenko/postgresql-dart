import 'dart:async';

/// Waits up to [timeout] for [future], then throws a [TimeoutException].
///
/// `.timeout()` doesn't cancel [future]. A value that it delivers after we
/// gave up goes to [release], since nothing else would ever reach it. When we
/// give up, [onGiveUp] gets the future of that release, which never fails.
Future<T> timeoutReleasingLate<T>(
  Future<T> future,
  Duration timeout, {
  required FutureOr<void> Function(T value) release,
  String message = 'Future not completed',
  void Function(Future<void> released)? onGiveUp,
}) {
  return future.timeout(
    timeout,
    onTimeout: () {
      final released = future
          .then(release)
          .then<void>((_) {}, onError: (Object _) {});
      onGiveUp?.call(released);
      throw TimeoutException(message, timeout);
    },
  );
}
