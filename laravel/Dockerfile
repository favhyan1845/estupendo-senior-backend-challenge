# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Laravel dependencies (production)
# ---------------------------------------------------------------------------
FROM composer:2 AS vendor

WORKDIR /app
ENV COMPOSER_ALLOW_SUPERUSER=1

COPY composer.json composer.lock ./
RUN composer install \
        --no-dev \
        --no-scripts \
        --no-autoloader \
        --prefer-dist \
        --no-interaction \
        --no-progress

COPY . .
RUN composer dump-autoload --optimize --no-dev --no-interaction

# ---------------------------------------------------------------------------
# Laravel dependencies (tests: PHPUnit, Faker, Collision, Pint)
# ---------------------------------------------------------------------------
FROM composer:2 AS vendor-dev

WORKDIR /app
ENV COMPOSER_ALLOW_SUPERUSER=1

COPY composer.json composer.lock ./
RUN composer install \
        --no-scripts \
        --no-autoloader \
        --prefer-dist \
        --no-interaction \
        --no-progress

COPY . .
RUN composer dump-autoload --optimize --no-interaction

# ---------------------------------------------------------------------------
# Runtime image
# ---------------------------------------------------------------------------
FROM php:8.3-cli-alpine AS runtime

RUN apk add --no-cache bash $PHPIZE_DEPS linux-headers \
    && docker-php-ext-install -j"$(nproc)" pdo_mysql pcntl bcmath opcache \
    && pecl install redis \
    && docker-php-ext-enable redis \
    && apk del $PHPIZE_DEPS linux-headers

COPY docker/php.ini /usr/local/etc/php/conf.d/99-app.ini

WORKDIR /var/www/html
COPY --from=vendor /app /var/www/html
COPY docker/entrypoint.sh /usr/local/bin/entrypoint

RUN chmod +x /usr/local/bin/entrypoint \
    && mkdir -p storage/framework/cache storage/framework/sessions storage/framework/views storage/logs bootstrap/cache \
    && chown -R www-data:www-data storage bootstrap/cache

USER www-data

EXPOSE 8000

ENTRYPOINT ["entrypoint"]
CMD ["php", "artisan", "serve", "--host=0.0.0.0", "--port=8000", "--no-reload"]

# ---------------------------------------------------------------------------
# Testing image: same runtime + dev dependencies
# ---------------------------------------------------------------------------
FROM runtime AS testing

USER root
COPY --from=vendor-dev /app/vendor /var/www/html/vendor

# The runtime image was built with --no-dev, so its package manifest does not
# know about PHPUnit/Collision. Regenerate it with the dev vendor in place.
RUN php artisan package:discover --ansi \
    && chown -R www-data:www-data bootstrap/cache

# phpdotenv tries to read .env on every boot; a missing file surfaces as a
# PHPUnit warning (suppressed warning converted by PHPUnit 11). A committed
# .env.testing keeps the test image self-contained and deterministic.
RUN printf '%s\n' \
        'APP_ENV=testing' \
        'APP_KEY=base64:1b2ZTO+Y/bjmwzSjsz1ffQHnYwjRR398knldvmJjdms=' \
        'LOG_CHANNEL=stderr' \
        > /var/www/html/.env.testing \
    && chown www-data:www-data /var/www/html/.env.testing

USER www-data
