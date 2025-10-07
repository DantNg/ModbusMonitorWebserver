#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Boids Demo (matplotlib) — flocking 2D với separation / alignment / cohesion
Phím tắt:
  [Space]  : Pause/Resume
  r        : Reset ngẫu nhiên
  v        : Bật/tắt vector vận tốc (quiver)
  o        : Bật/tắt chướng ngại hình tròn
  b        : Bật/tắt wrap biên (wrap-around vs. tường dội)
  +/-      : Tăng/Giảm tốc tối đa
  [ và ]   : Tăng/Giảm lực tối đa (max_force)
"""

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib.widgets import Slider

# ============ Cấu hình mặc định ============
W, H = 10.0, 7.0          # kích thước không gian (đơn vị tuỳ ý)
N = 120                    # số lượng boids
INIT_SPEED = 1.5
MAX_SPEED = 3.0
MAX_FORCE = 0.06

SEP_RAD = 0.25
ALI_RAD = 0.7
COH_RAD = 0.7

SEP_W = 1.4
ALI_W = 0.9
COH_W = 0.8

DT = 0.05                 # bước thời gian

# Obstacle
OBSTACLE_ON = False
OBS_CENTER = np.array([W*0.5, H*0.5])
OBS_R = 0.75
OBS_REPULSE = 2.0         # lực đẩy khi vào gần obstacle

WRAP = True               # wrap biên hay nẩy tường

# ============ Trạng thái toàn cục ============
rng = np.random.default_rng(123)
pos = rng.random((N, 2)) * np.array([W, H])
angles = rng.random(N) * 2*np.pi
vel = np.c_[np.cos(angles), np.sin(angles)] * INIT_SPEED

paused = False
show_quiver = True
max_speed = MAX_SPEED
max_force = MAX_FORCE
sep_rad, ali_rad, coh_rad = SEP_RAD, ALI_RAD, COH_RAD
sep_w, ali_w, coh_w = SEP_W, ALI_W, COH_W
obstacle_on = OBSTACLE_ON
wrap_mode = WRAP

# ============ Tiện ích ============
def limit_vec_length(v, max_len):
    n = np.linalg.norm(v, axis=1, keepdims=True) + 1e-9
    scale = np.minimum(1.0, max_len / n)
    return v * scale

def reset_state():
    global pos, vel
    pos[:] = rng.random((N, 2)) * np.array([W, H])
    ang = rng.random(N) * 2*np.pi
    vel[:] = np.c_[np.cos(ang), np.sin(ang)] * INIT_SPEED

# ============ Cập nhật boids ============
def step():
    global pos, vel
    # Ma trận khoảng cách O(N^2)
    diff = pos[:, None, :] - pos[None, :, :]              # (N,N,2)
    dist = np.linalg.norm(diff, axis=2) + 1e-9            # (N,N)
    np.fill_diagonal(dist, np.inf)                        # bỏ chính nó

    # Láng giềng theo từng bán kính
    neigh_sep = dist < sep_rad
    neigh_ali = dist < ali_rad
    neigh_coh = dist < coh_rad

    # Separation: đẩy khỏi lân cận gần
    # vector từ láng giềng -> ta là -diff; chuẩn hóa và sum
    inv = np.where(neigh_sep, 1.0 / dist, 0.0)            # (N,N)
    steer_sep = (diff * inv[..., None]).sum(axis=1) * (-1.0)

    # Alignment: hướng về trung bình vận tốc của láng giềng
    ali_count = neigh_ali.sum(axis=1, keepdims=True)      # (N,1)
    vel_sum = neigh_ali @ vel                              # (N,2)
    avg_vel = np.where(ali_count > 0, vel_sum / ali_count, 0.0)
    steer_ali = avg_vel - vel

    # Cohesion: hướng về trung bình vị trí láng giềng
    coh_count = neigh_coh.sum(axis=1, keepdims=True)
    pos_sum = neigh_coh @ pos
    avg_pos = np.where(coh_count > 0, pos_sum / coh_count, 0.0)
    steer_coh = avg_pos - pos

    # Obstacle repulsion (tuỳ chọn)
    if obstacle_on:
        to_obs = pos - OBS_CENTER
        d = np.linalg.norm(to_obs, axis=1, keepdims=True) + 1e-9
        inside = d < (OBS_R * 1.2)
        repulse = np.where(inside, to_obs / d * OBS_REPULSE, 0.0)
    else:
        repulse = 0.0

    # Tổng lực + giới hạn lực
    acc = sep_w * steer_sep + ali_w * steer_ali + coh_w * steer_coh + repulse
    acc = limit_vec_length(acc, max_force)

    # Cập nhật vận tốc & giới hạn tốc độ
    vel = vel + acc
    vel = limit_vec_length(vel, max_speed)

    # Cập nhật vị trí + xử lý biên
    pos = pos + vel * DT
    if wrap_mode:
        pos[:, 0] = np.mod(pos[:, 0], W)
        pos[:, 1] = np.mod(pos[:, 1], H)
    else:
        # dội tường
        hit_left = pos[:, 0] < 0
        hit_right = pos[:, 0] > W
        hit_bottom = pos[:, 1] < 0
        hit_top = pos[:, 1] > H
        if np.any(hit_left | hit_right):
            vel[hit_left | hit_right, 0] *= -1
        if np.any(hit_bottom | hit_top):
            vel[hit_bottom | hit_top, 1] *= -1
        pos[:, 0] = np.clip(pos[:, 0], 0, W)
        pos[:, 1] = np.clip(pos[:, 1], 0, H)

# ============ Visualization ============
plt.style.use('default')
fig = plt.figure(figsize=(10, 7.8))
gs = fig.add_gridspec(6, 4, height_ratios=[20,1,1,1,1,1], hspace=0.5, wspace=0.4)

ax = fig.add_subplot(gs[0:1, :])
ax.set_xlim(0, W); ax.set_ylim(0, H)
ax.set_aspect('equal', adjustable='box')
ax.set_title("Boids Flocking — Separation / Alignment / Cohesion (matplotlib)")

# scatter: vị trí boids
scat = ax.scatter(pos[:, 0], pos[:, 1], s=15, alpha=0.85)
# quiver: vector vận tốc
quiv = ax.quiver(pos[:, 0], pos[:, 1], vel[:, 0], vel[:, 1], angles='xy', scale_units='xy', scale=10)
# obstacle
obs_patch = plt.Circle(OBS_CENTER, OBS_R, fill=False, linestyle='--', linewidth=1.5, alpha=0.6)
if obstacle_on:
    ax.add_patch(obs_patch)

# Sliders
ax_sep_w  = fig.add_subplot(gs[1, 0])
ax_ali_w  = fig.add_subplot(gs[1, 1])
ax_coh_w  = fig.add_subplot(gs[1, 2])
ax_speed  = fig.add_subplot(gs[1, 3])

ax_sep_r  = fig.add_subplot(gs[2, 0])
ax_ali_r  = fig.add_subplot(gs[2, 1])
ax_coh_r  = fig.add_subplot(gs[2, 2])
ax_force  = fig.add_subplot(gs[2, 3])

s_sep_w = Slider(ax_sep_w, 'Sep W', 0.0, 3.0, valinit=sep_w)
s_ali_w = Slider(ax_ali_w, 'Ali W', 0.0, 3.0, valinit=ali_w)
s_coh_w = Slider(ax_coh_w, 'Coh W', 0.0, 3.0, valinit=coh_w)
s_speed = Slider(ax_speed, 'Max v', 0.5, 6.0, valinit=max_speed)

s_sep_r = Slider(ax_sep_r, 'Sep R', 0.05, 1.0, valinit=sep_rad)
s_ali_r = Slider(ax_ali_r, 'Ali R', 0.2, 2.0, valinit=ali_rad)
s_coh_r = Slider(ax_coh_r, 'Coh R', 0.2, 2.0, valinit=coh_rad)
s_force = Slider(ax_force, 'Max F', 0.01, 0.2, valinit=max_force)

def on_slider(_):
    global sep_w, ali_w, coh_w, max_speed, sep_rad, ali_rad, coh_rad, max_force
    sep_w   = float(s_sep_w.val)
    ali_w   = float(s_ali_w.val)
    coh_w   = float(s_coh_w.val)
    max_speed = float(s_speed.val)
    sep_rad = float(s_sep_r.val)
    ali_rad = float(s_ali_r.val)
    coh_rad = float(s_coh_r.val)
    max_force = float(s_force.val)

for s in (s_sep_w, s_ali_w, s_coh_w, s_speed, s_sep_r, s_ali_r, s_coh_r, s_force):
    s.on_changed(on_slider)

# ============ Events ============
def on_key(event):
    global paused, show_quiver, obstacle_on, wrap_mode, max_speed, max_force
    if event.key == ' ':
        paused = not paused
    elif event.key == 'r':
        reset_state()
    elif event.key == 'v':
        show_quiver = not show_quiver
        quiv.set_visible(show_quiver)
    elif event.key == 'o':
        obstacle_on = not obstacle_on
        if obstacle_on:
            if obs_patch not in ax.patches:
                ax.add_patch(obs_patch)
        else:
            if obs_patch in ax.patches:
                obs_patch.remove()
    elif event.key == 'b':
        wrap_mode = not wrap_mode
    elif event.key == '+':
        max_speed = min(10.0, max_speed + 0.2); s_speed.set_val(max_speed)
    elif event.key == '-':
        max_speed = max(0.2, max_speed - 0.2); s_speed.set_val(max_speed)
    elif event.key == '[':
        max_force = max(0.005, max_force - 0.005); s_force.set_val(max_force)
    elif event.key == ']':
        max_force = min(0.5, max_force + 0.005); s_force.set_val(max_force)

fig.canvas.mpl_connect('key_press_event', on_key)

# ============ Animation ============
def update(_frame):
    if not paused:
        step()
        scat.set_offsets(pos)
        if show_quiver:
            quiv.set_offsets(pos)
            # Quiver expects U,V as 2D arrays; set_UVC là API để cập nhật
            quiv.set_UVC(vel[:, 0], vel[:, 1])
    return scat, quiv

ani = FuncAnimation(fig, update, interval=16, blit=False)

# Hướng dẫn nhỏ
txt = (
    "[Space]=Pause  r=Reset  v=Vel.vec  o=Obstacle  b=Wrap  "
    "+/-=Max speed  [ / ]=Max force\n"
    "Kéo sliders để chỉnh weights/radii."
)
fig.text(0.02, 0.01, txt, fontsize=9)

plt.show()
