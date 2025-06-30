# Additional Grafana Security Hardening

## 1. Create Read-Only User for Public Access

Instead of using admin credentials, create a viewer-only user:

```bash
# After Grafana is running, exec into container
docker exec -it qbtc-grafana-prod /bin/bash

# Use Grafana CLI to create read-only user
grafana-cli admin create-user --name viewer --password ViewerPassword123! --email viewer@example.com
grafana-cli admin set-user-role --name viewer --role Viewer
```

## 2. Disable Grafana Editing Completely

Add these to your docker-compose environment:

```yaml
environment:
  # Existing settings...
  - GF_FEATURE_TOGGLES_ENABLE=publicDashboards
  - GF_AUTH_DISABLE_SIGNOUT_MENU=true
  - GF_EXPLORE_ENABLED=false
  - GF_USERS_VIEWERS_CAN_EDIT=false
  - GF_DISABLE_SANITIZE_HTML=false
  - GF_PANELS_DISABLE_ANIMATIONS=false
  - GF_DASHBOARDS_MIN_REFRESH_INTERVAL=10s
```

## 3. IP Whitelist for Admin Access

Add to nginx config to restrict admin paths to specific IPs:

```nginx
# Admin access only from specific IPs
location ~ ^/(api/admin|admin|api/datasources|api/plugins|api/users|api/orgs) {
    allow 192.168.1.100;  # Your admin IP
    allow 10.0.0.5;       # Another admin IP
    deny all;
    
    proxy_pass http://grafana:3000;
    # ... rest of proxy config
}
```

## 4. Implement Fail2Ban

Create `/etc/fail2ban/jail.local`:

```ini
[grafana-auth]
enabled = true
filter = grafana-auth
port = https,http
logpath = /var/log/nginx/access.log
maxretry = 5
bantime = 3600
```

Create `/etc/fail2ban/filter.d/grafana-auth.conf`:

```ini
[Definition]
failregex = ^<HOST> .* "POST /login HTTP/.*" 401
            ^<HOST> .* "GET .* HTTP/.*" 401
ignoreregex =
```

## 5. Use Grafana API Keys Instead of User Login

For truly public dashboards, use anonymous access with specific dashboard permissions:

```yaml
environment:
  - GF_AUTH_ANONYMOUS_ENABLED=true
  - GF_AUTH_ANONYMOUS_ORG_NAME=Public
  - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
  - GF_AUTH_BASIC_ENABLED=false
  - GF_AUTH_DISABLE_LOGIN_FORM=true
```

## 6. Regular Security Updates

Add to your maintenance routine:

```bash
# Weekly update check
docker pull grafana/grafana:latest
docker-compose -f docker-compose.bootstrap.yml up -d grafana
```

## 7. Monitoring and Alerts

Set up alerts for:
- Failed login attempts
- Unusual API access patterns
- High request rates from single IPs
- Access to forbidden paths

## 8. Content Security Policy

Add stricter CSP headers in nginx:

```nginx
add_header Content-Security-Policy "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https://grafana.com; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none';" always;
```