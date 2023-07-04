# Auto-generated with massprojectupdater
# set with `docker build --build-arg` from group/project environment variable
FROM 627987680837.dkr.ecr.eu-central-1.amazonaws.com/prd/yolt-openjdk-17:693517
# The jar was exploded in the gitlab pipeline. This gives us more speed due to re-usage of the dependencies in the
# lib directory. They are cached locally on the gitlab runner (which uses a volume on the k8s worker).
COPY BOOT-INF/lib /app/lib
COPY META-INF /app/META-INF
COPY BOOT-INF/classes /app

# Read the manifest correctly by joining lines (72 is max line length next line starts with a space, this belongs to the same line)
RUN echo $(cat /app/META-INF/MANIFEST.MF | awk ' \
  /\r$/   { sub("\r$", ""); } \
  /^[^ ]/ { print manifest; manifest=$0 } \
  END     { print manifest } \
  /^ /    { sub(" *", ""); manifest = manifest $0 }' | \
  awk '/Start-Class/ {print $2}') >> /home/yolt/MainClassName

COPY --chown=yolt:yolt startup.sh /home/yolt/startup.sh
RUN chmod 500 /home/yolt/startup.sh

ENTRYPOINT ["/home/yolt/startup.sh"]
