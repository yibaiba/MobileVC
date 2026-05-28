import 'package:flutter/material.dart';

class IOSTokens {
  static const double radiusCard = 20;
  static const double radiusInput = 16;
  static const double radiusButton = 14;
  static const double radiusSheet = 28;
  static const Color iosBlue = Color(0xFF007AFF);
  static const Color iosDarkBlue = Color(0xFF0A84FF);
  static const Color iosBgLight = Color(0xFFF2F2F7);
  static const Color iosBgDark = Color(0xFF000000);
  static const Color iosSurfaceLight = Color(0xFFFFFFFF);
  static const Color iosSurfaceDark = Color(0xFF1C1C1E);
  static const Color iosRed = Color(0xFFFF3B30);
}

class AppTheme {
  static ThemeData light() {
    const seed = Color(0xFFCC785C);
    final scheme =
        ColorScheme.fromSeed(seedColor: seed, brightness: Brightness.light);
    return _build(
      scheme: scheme,
      scaffoldBackground: const Color(0xFFFAF9F6),
      surface: const Color(0xFFF5F4F1),
      snackBarBackground: const Color(0xFF2D2B28),
      snackBarForeground: const Color(0xFFFAF9F6),
      outlineAlpha: 0.12,
    );
  }

  static ThemeData dark() {
    const seed = IOSTokens.iosDarkBlue;
    final scheme =
        ColorScheme.fromSeed(seedColor: seed, brightness: Brightness.dark);
    return _build(
      scheme: scheme,
      scaffoldBackground: IOSTokens.iosBgDark,
      surface: IOSTokens.iosSurfaceDark,
      snackBarBackground: const Color(0xFF2C2C2E),
      snackBarForeground: Colors.white,
      outlineAlpha: 0.18,
    );
  }

  static ThemeData _build({
    required ColorScheme scheme,
    required Color scaffoldBackground,
    required Color surface,
    required Color snackBarBackground,
    required Color snackBarForeground,
    required double outlineAlpha,
  }) {
    final outline = scheme.outlineVariant.withValues(alpha: outlineAlpha);
    return ThemeData(
      useMaterial3: true,
      colorScheme: scheme,
      brightness: scheme.brightness,
      scaffoldBackgroundColor: scaffoldBackground,
      dividerColor: outline,
      pageTransitionsTheme: const PageTransitionsTheme(
        builders: {
          TargetPlatform.iOS: CupertinoPageTransitionsBuilder(),
          TargetPlatform.android: CupertinoPageTransitionsBuilder(),
        },
      ),
      appBarTheme: AppBarTheme(
        centerTitle: false,
        backgroundColor: Colors.transparent,
        foregroundColor: scheme.onSurface,
        elevation: 0,
        scrolledUnderElevation: 0,
        surfaceTintColor: Colors.transparent,
      ),
      inputDecorationTheme: InputDecorationTheme(
        filled: true,
        fillColor: surface,
        contentPadding:
            const EdgeInsets.symmetric(horizontal: 16, vertical: 14),
        border: OutlineInputBorder(
          borderRadius: BorderRadius.circular(IOSTokens.radiusInput),
          borderSide: BorderSide(color: outline),
        ),
        enabledBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(IOSTokens.radiusInput),
          borderSide: BorderSide(color: outline),
        ),
        focusedBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(IOSTokens.radiusInput),
          borderSide: BorderSide(color: scheme.primary, width: 1.2),
        ),
      ),
      cardTheme: CardThemeData(
        elevation: 0,
        color: surface,
        margin: EdgeInsets.zero,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(IOSTokens.radiusCard),
          side: BorderSide(color: outline),
        ),
      ),
      filledButtonTheme: FilledButtonThemeData(
        style: FilledButton.styleFrom(
          minimumSize: const Size(0, 46),
          shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(IOSTokens.radiusButton)),
        ),
      ),
      outlinedButtonTheme: OutlinedButtonThemeData(
        style: OutlinedButton.styleFrom(
          minimumSize: const Size(0, 44),
          shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(IOSTokens.radiusButton)),
          side: BorderSide(color: outline),
        ),
      ),
      chipTheme: ChipThemeData(
        backgroundColor: scheme.surfaceContainerHighest,
        selectedColor: scheme.primaryContainer,
        side: BorderSide(color: outline),
        labelStyle: TextStyle(
          color: scheme.onSurface,
          fontWeight: FontWeight.w600,
        ),
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(999)),
      ),
      dividerTheme: DividerThemeData(
        color: outline,
        thickness: 0.5,
        space: 0.5,
      ),
      snackBarTheme: SnackBarThemeData(
        behavior: SnackBarBehavior.floating,
        backgroundColor: snackBarBackground,
        contentTextStyle: TextStyle(color: snackBarForeground),
        shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      ),
    );
  }
}
