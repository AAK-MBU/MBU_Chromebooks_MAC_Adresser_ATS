# MBU Chromebooks MAC Adresser ATS

---

## 🔍 Overview

This project defines a Python-based Automation Server (ATS) process developed for Aarhus Kommune’s MBU automation platform.
The robot automates a process fetches registered Chromebooks for Aarhus Kommune's customer id, and uploads the MAC adresse for each of them to a database.

---

## ⚙️ Main Responsibilities
- Fetch registered Chromebooks
- Retrieve the MAC adress, device ID, and the serial number for each chromebook
- Upload data to specified table in RPA-database

---

## 🧠 How it works

1. The robot is started in ATS
2. Retrieves parameters from Constants-table
3. Uses these to fetch list of Chromebooks, leveraging Googles API
4. Retrieves specific data for each Chromebook
5. Updates a database, containing all new and former registered Chromebooks

---

## 🧩 Structure

- main.py – Entry point for all subflows
- helpers/ – Shared utilities for ATS API, configs, and queue management
- processes/ – Core logic for queue handling, item processing, and error management