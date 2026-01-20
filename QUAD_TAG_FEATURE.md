# Quad Tag Card Feature

## Overview
Added new "Add Quad Tag Card" option to subdashboards that allows displaying 4 tags in a single card with 2x2 grid layout.

## Usage
1. Go to any subdashboard detail page
2. Click Settings dropdown (gear icon)
3. Select "Add Quad Tag Card"
4. Select exactly 4 different tags:
   - Tag 1 (Top Left)
   - Tag 2 (Top Right)
   - Tag 3 (Bottom Left)
   - Tag 4 (Bottom Right)
5. Choose existing group or create new group
6. Click "Add Quad Tag Card"

## Features
- 2x2 grid layout with tag names and values arranged horizontally
- Real-time value updates via WebSocket
- Support for both light and dark themes
- Validation to prevent duplicate tag selection
- Option to create new group or add to existing group
- Delete quad card functionality for admin users

## Technical Implementation
- Added `card_type` column to `subdash_tag_groups` table
- Backend endpoint: `/subdash/{sid}/add_quad_tag`
- CSS classes: `.quad-tag-card`, `.quad-tag-grid`, `.quad-tag-item`
- JavaScript form handler with duplicate validation
- Database migration to support card types

## Layout
```
┌─────────────────┬─────────────────┐
│Tag1Name Value1  │Tag2Name Value2  │
│                 │                 │
┌─────────────────┬─────────────────┐
│Tag3Name Value3  │Tag4Name Value4  │
│                 │                 │
├─────────────────┼─────────────────┤
```