SOURCE_BASE_DIR=s3://atrax-configuration-management/packages/${MODULE_NAME}
DEST_BASE_DIR=/usr/local/packages/${MODULE_NAME}

[ ! -d ${DEST_BASE_DIR} ] && mkdir -p "$DEST_BASE_DIR"

if [ -e "$DEST_BASE_DIR/debian_env_setup.sh" ]
then
    debian_env_setup_mod=`stat -c %Y "$DEST_BASE_DIR/debian_env_setup.sh"`
    aws s3 sync --exact-timestamps "$SOURCE_BASE_DIR" "$DEST_BASE_DIR"
    new_debian_env_setup_mod=`stat -c %Y "$DEST_BASE_DIR/debian_env_setup.sh"`
    if [ "$debian_env_setup_mod" -ne "$new_debian_env_setup_mod" ]
    then
        bash "$DEST_BASE_DIR/debian_env_setup.sh"
    fi
else
    aws s3 sync --exact-timestamps "$SOURCE_BASE_DIR" "$DEST_BASE_DIR"
    bash "$DEST_BASE_DIR/debian_env_setup.sh"
fi

tar -xf "$DEST_BASE_DIR/$MODULE_NAME.tar" -C "$DEST_BASE_DIR"
