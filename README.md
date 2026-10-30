# WILDERNESS Multiplayer

Your original game (`public/index.html`) + a Node.js / Socket.IO server.

## Run locally
    npm install
    npm start
Open http://localhost:3000 in two browser windows -> MULTIPLAYER -> CREATE ROOM in one, JOIN ROOM with the code in the other.

## GitHub
    git init && git add . && git commit -m "WILDERNESS multiplayer"
    git branch -M main
    git remote add origin https://github.com/YOUR-USER/wilderness-multiplayer.git
    git push -u origin main

## Render (Web Service)
Runtime Node | Build Command `npm install` | Start Command `npm start` | Health check `/health`
(render.yaml is included, so "New > Blueprint" also works.)
URL: https://<your-service-name>.onrender.com  (HTTPS => secure WebSocket automatically)
Free instances sleep when idle; the first load can take ~30 s.

## Shared vs personal
Shared: players, time of day, weather, trees/rocks/bushes/logs, buildings, chests, campfires, tables, dropped items, wildlife + kills, chat.
Personal (like instanced loot): inventory, quests, coins, caves, ruins/camp loot, moon rocket, portals. Other players' machines are scenery.
Singleplayer is unchanged and still saves locally.
