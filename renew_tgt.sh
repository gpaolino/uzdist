#!/bin/bash

# -----------------------------
# Kerberos
# Should use the kinit -kt with keytab file
# -----------------------------
echo "MyPassword" | kinit USERNAME@DOMAIN.DOM
