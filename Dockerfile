FROM archlinux/archlinux:base
ADD . /opt/sync_guide
ENV COVER_PDF_PATH /opt/sync_guide/resources/cover.pdf
RUN pacman -Syu
RUN pacman -S --noconfirm python python-pip
RUN pip install --break-system-packages -r /opt/sync_guide/requirements.txt
CMD python /opt/sync_guide/main.py
